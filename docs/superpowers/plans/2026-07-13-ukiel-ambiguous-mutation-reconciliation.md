# Ukiel Plan 43: Ambiguous Mutation Reconciliation and Non-Reused Fencing Plan

> **For agentic workers:** Execute this plan task-by-task. Use checkbox (`- [ ]`)
> progress, run each task's focused tests before its commit, and do not begin
> Plan 8 pipeline/MV work in this plan.

**Status:** **Executed 2026-07-13.** HA Phase 2 delivered; the production gate on
active-active compaction is closed. Eight task commits; `make e2e-ha` green with
S11 passing 8 consecutive runs. What this plan explicitly did *not* do remains
open: no horizontal ingest ownership, no GC singleton lease, no pipeline/MV
entities, no Aurora RTO measurement (HA phases 3 and 4).

**Goal:** Make a catalog mutation with a lost commit acknowledgement decidable.
Every retryable ingest, compaction, and deletion mutation gets a stable logical
identity and fingerprint. The catalog records that identity atomically with the
commit, rejects key reuse for different intent, and exposes an authoritative
lookup. Plan 42's role supervisor carries an ambiguous identity across worker
reconstruction and does not call the role healthy until lookup says either
`Committed(commit_id)` or `Absent`. Before adding that longer-lived token path,
fix issue 0013 so a released compaction lease generation can never be reused.

**Why now:** Plans 41 and 42 delivered the scheduling and recovery substrate:
partition leases, transactional fencing, monotonic cursors, live-state replans,
typed catalog errors, bounded reconnect, and role-level `Reconciling`. They
deliberately stop at an unknown result when the connection dies around
`COMMIT`. Rebuilding from offsets/live parts is safe today, but it cannot answer
whether one particular operation landed, and production active-active
compaction remains gated on that answer. Issue 0013 also permits the sequence
`(partition, owner A, generation 1) -> release -> (partition, owner A,
generation 1)`; reconciliation is exactly the kind of lifecycle that can retain
the old token long enough to make that ABA bug reachable.

## Decisions fixed by this plan

### 1. Lease generations are tenancy tokens, never row-local counters

Migration `0010` creates a catalog-owned PostgreSQL sequence. A new lease row
and every expired/released tenancy take `nextval`; an idempotent reacquisition
by the same unexpired owner preserves the held generation. Clean release still
deletes the row. Sequence gaps from conflicts, rollbacks, or idempotent acquire
attempts are harmless: the token needs ordering and non-reuse, not density.

The migration initializes the sequence strictly above every generation already
present. If another plan lands a migration first, renumber this plan's `0010`
and `0011` together; never reuse an applied migration number.

### 2. Identity names logical intent, not one physical attempt

Add a shared `OperationIdentity` containing:

```rust
pub struct OperationIdentity {
    hypertable_id: HypertableId,
    key: String,
    fingerprint: [u8; 32],
}
```

Callers construct it with versioned, domain-separated factories. Canonical
bytes use explicit type tags and length prefixes, never `Debug`, unordered map
iteration, or implicit `serde_json` enum encoding. Hash those bytes with BLAKE3
and format the key as `ukiel/<domain>/v<version>/<hex fingerprint>`. Store the
32-byte fingerprint separately even though the safe factories derive the key
from it: the catalog boundary must still detect a buggy/legacy caller or direct
SQL that reuses a key with different intent.

Canonical inputs are:

| domain | canonical logical intent |
|---|---|
| ingest v1 | sorted `(topic, partition, first_offset, last_offset)` ranges plus the ingest transformation version |
| compaction v1 | output level, sorted input part IDs, canonical partition value, table schema, sort key, placement, and compaction transformation version |
| delete-key v1 | packing key, sorted input part IDs, table schema/sort/placement, and deletion transformation version |
| future MV | reserved `mv` domain; Plan 8 adds pipeline ID plus source commit ID using this same builder |

Do **not** include generated object paths, upload UUIDs, lease owner/generation,
attempt number, process ID, or wall-clock time. Two workers may produce
different object paths for the same compaction intent. The first committed
outputs are authoritative; the losing attempt's pending objects remain ordinary
GC-cleanable orphans.

Canonical JSON recursively sorts object keys and preserves array order, scalar
types, and JSON-null. Part IDs and offset ranges are sorted and duplicate
canonical entries are rejected. Version constants change whenever a code or
policy change can alter the logical transformation.

### 3. One catalog record answers the outcome

Keep `commits.idempotency_key` as the stored operation key to avoid a noisy
rename and migration. Migration `0011` adds nullable
`operation_fingerprint BYTEA` with a 32-byte check. New application writes
either set both key/fingerprint or set neither. Historical keyed rows cannot be
safely backfilled because their intent is not recoverable; a collision with one
fails loudly instead of being guessed. New canonical key prefixes make such a
collision practically avoidable.

The public lookup is:

```rust
pub enum OperationLookup {
    Absent,
    Committed(CommitId),
}

pub async fn lookup_operation(
    &self,
    identity: &OperationIdentity,
) -> Result<OperationLookup, CatalogError>;
```

Lookup scopes by hypertable and key and compares the stored fingerprint before
returning `Committed`. A mismatch, including a historical NULL fingerprint,
returns `CatalogError::OperationKeyCollision`; it never returns
`AlreadyApplied`.

Mutation insertion, fingerprint storage, part changes, pending-object clearing,
offset advancement, and commit visibility remain one PostgreSQL transaction.
On a key conflict, compare the fingerprint and return the original commit ID
without applying the supplied payload. For a leased REPLACE, this replay lookup
still happens **before** lease validation: durable work wins over a lease that
expired after the commit.

### 4. Only an error at the commit boundary is ambiguous

An acquire/begin/read failure before mutation execution remains a transient
pre-outcome error. A conflict, `OffsetRace`, lease loss, validation error, or
failed statement before `COMMIT` retains its existing semantic class. A
retryable-database error such as SQLSTATE `40001`/`40P01` is also a definite
failed transaction and retains `RetryableDatabase`. Only wrap a transport-class
error returned by `tx.commit().await` as:

```rust
CatalogError::AmbiguousMutation {
    identity: OperationIdentity,
    source: sqlx::Error,
}
```

Only identified mutations may use this variant. It classifies as transport for
Plan 42 recovery while preserving the identity. An unidentified mutation that
reaches the same boundary returns a separate
`CatalogError::UnidentifiedAmbiguousMutation` with a permanent class: its
outcome is unknown but it cannot be reconciled. Do not invent a key after seeing
the error. All production ingest, compaction, and deletion callers must be on
the identified APIs before S11 is accepted.

### 5. Reconciliation belongs between reconnect and worker rebuild

Extend Plan 42's typed `RoleFailure` boundary so the supervisor can clone an
ambiguous `OperationIdentity` before dropping the failed worker. It then:

1. marks the role `Degraded` and waits for the writer endpoint as today;
2. marks the role `Reconciling`;
3. calls `lookup_operation` with bounded, jittered reconnect behavior;
4. records `Committed(id)` or `Absent` as the authoritative outcome;
5. only then rebuilds the worker from offsets/live parts and lets its existing
   ready signal move the role to `Healthy`.

`Absent` is not permission to replay stale in-memory payload. The old worker is
already gone. Ingest rereads catalog offsets and Kafka; compaction rereads live
parts and reacquires a lease; deletion callers reissue the request and derive a
new/current plan. `Committed` similarly reloads authority rather than trying to
restore the previous worker's local result value.

Direct library callers outside `ukield` receive the typed ambiguous error and
can call `lookup_operation` themselves. The low-level catalog does not add an
unbounded retry loop.

## Scope boundary

This plan does not add horizontal ingest ownership, a GC singleton lease,
pipeline/MV entities, cross-region catalog semantics, a durable external
deletion request table, or generic retry around arbitrary SQL. It does not make
object-store state authoritative and does not preserve worker buffers/plans
across recovery. Plan 8 will use the operation identity contract for MVs after
this plan lands; managed Aurora/RDS Proxy RTO proof remains HA Phase 4.

## Prerequisites and global constraints

- Plans 41 and 42 and issue 0012 are executed.
- Preserve optimistic REPLACE conflict checking; leases only fence scheduling.
- Preserve transactional Kafka offsets and pending-object clearing.
- Preserve Plan 42's lazy pool, role-local failure containment, readiness, and
  fail-closed GC behavior.
- Rust edition 2024, toolchain >= 1.96. Keep DataFusion 54, Arrow/Parquet 58.3,
  and object_store 0.13 pinned in lockstep.
- Add `blake3 = "1"` once at the workspace root and consume it from
  `ukiel-core`; add no hashing implementation inside Ukiel.
- Unit tests must run without Docker. Catalog integration and S11 require
  Docker/testcontainers.
- Every task ends with focused tests. Before each commit run
  `cargo fmt --check` and relevant Clippy targets with warnings denied.
- Conventional commits; no AI attribution.

---

### Task 1: Fix issue 0013 with non-reused lease generations

**Files:**
- Create: `crates/ukiel-catalog/migrations/0010_compaction_lease_generation_seq.sql`
- Modify: `crates/ukiel-catalog/src/leases.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Migration and behavior:**

- Create `compaction_lease_generation_seq AS BIGINT`, owned by the catalog
  schema owner, and initialize its next value above
  `coalesce(max(compaction_leases.generation), 0)`.
- Fresh insert and expired takeover use `nextval`. Same-owner, unexpired
  reacquisition returns the current generation unchanged.
- Keep the one-statement `INSERT ... ON CONFLICT DO UPDATE` claim and the
  one-winner property. It is acceptable for attempted acquisitions to consume
  unused sequence values.
- Keep exact owner/generation renewal and release predicates. Clean release
  deletes immediately; the abandoned-lease metric continues to see only active
  or expired rows.

- [x] **Step 1: Write failing regression tests.** Same owner release/reacquire
  gets a strictly greater generation; different owner release/reacquire does
  too; the pre-release token cannot renew, release, or pass fenced REPLACE.
- [x] **Step 2: Add migration-upgrade coverage.** Seed generations before
  applying `0010`, migrate, and assert the first new tenancy is greater than
  the old maximum. Pin fresh-database migration as well.
- [x] **Step 3: Implement sequence-backed acquisition.** Preserve idempotent
  reacquisition and simultaneous one-winner behavior under 32 contenders.
- [x] **Step 4: Verify:**

```bash
cargo test -p ukiel-catalog --test catalog_test compaction_lease
cargo test -p ukiel-catalog --test catalog_test same_owner
cargo clippy -p ukiel-catalog --all-targets -- -D warnings
```

- [x] **Step 5: Commit:**

```bash
git commit -m "fix: never reuse compaction lease fencing generations"
```

---

### Task 2: Add canonical logical operation identities

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/ukiel-core/Cargo.toml`
- Create: `crates/ukiel-core/src/operation.rs`
- Modify: `crates/ukiel-core/src/lib.rs`

**Required interfaces:**

- Add `OperationIdentity`, a versioned/domain-separated canonical encoder, and
  focused factories/helpers needed by ingest, compaction, and deletion.
- The encoder supports bytes, UTF-8 strings, signed integers, ordered lists,
  and canonical JSON. Each value is type-tagged and length-delimited.
- Expose identity fields through read-only accessors; normal callers cannot pair
  an arbitrary key with an unrelated fingerprint. The catalog rejects an
  identity whose hypertable differs from the mutation argument. Catalog
  collision tests may insert malformed rows with direct SQL.
- Key length is bounded and independent of input-part count because it contains
  the digest, not the canonical payload.

- [x] **Step 1: Write canonicalization vectors.** Reordered JSON object keys and
  reordered input part IDs yield the same identity; arrays with different
  order, different domains/versions, different levels, schemas, offsets, or
  placement yield different identities. Pin exact key/fingerprint test vectors
  so an accidental encoder change becomes a versioning decision.
- [x] **Step 2: Test malformed intent rejection.** Duplicate part IDs,
  duplicate/overlapping offset ranges, invalid ranges, empty required domains,
  and oversized keys fail before catalog I/O.
- [x] **Step 3: Implement the encoder and BLAKE3 dependency.** No generated
  path, UUID, lease field, timestamp, or process identity may enter the vectors.
- [x] **Step 4: Verify:**

```bash
cargo test -p ukiel-core operation
cargo clippy -p ukiel-core --all-targets -- -D warnings
```

- [x] **Step 5: Commit:**

```bash
git commit -m "feat: define canonical catalog operation identities"
```

---

### Task 3: Store fingerprints and expose authoritative lookup

**Files:**
- Create: `crates/ukiel-catalog/migrations/0011_operation_fingerprints.sql`
- Modify: `crates/ukiel-catalog/src/commit.rs`
- Modify: `crates/ukiel-catalog/src/error.rs`
- Modify: `crates/ukiel-catalog/src/error_tests.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`
- Modify direct callers/tests affected by the typed identity parameter

**API changes:**

- Replace `Option<&str>` idempotency arguments with
  `Option<&OperationIdentity>` on general `commit`; make an identity mandatory
  on `commit_with_offsets` and `commit_compaction_replace`.
- Add `OperationLookup` and `lookup_operation`.
- Add `OperationKeyCollision` as permanent input, `AmbiguousMutation` as a
  transport-class error carrying identity plus the SQLx source, and
  `UnidentifiedAmbiguousMutation` as a loud permanent failure for legacy/
  one-shot callers that omitted identity.
- Insert key and fingerprint together. A conflict reads commit ID and stored
  fingerprint in the same transaction, compares, and only then returns
  `AlreadyApplied`.
- In the leased path, compare/reconcile identity before `fence_lease` exactly
  as Plan 41 requires.
- Only wrap a **transport-class** error returned from the final transaction
  commit as ambiguous; retain the source and do not message-match it.
  `RetryableDatabase` remains a definite failed transaction.

- [x] **Step 1: Migration tests.** Fresh migration, upgrade with unkeyed and
  historical keyed commits, 32-byte constraint, and unchanged Plan 40 direct
  seeding.
- [x] **Step 2: Failing lookup/replay tests.** Absent, committed, same-key/same-
  fingerprint replay, same-key/different-fingerprint collision, historical NULL
  fingerprint collision, hypertable scoping, and explicit identity/mutation
  hypertable mismatch.
- [x] **Step 3: Failing transaction-order tests.** An applied leased REPLACE is
  returned after lease expiry/takeover; a new stale operation still gets
  `LeaseLost`; collision is reported before lease state and mutates nothing.
- [x] **Step 4: Implement schema, API, errors, and metrics labels.** Never put
  operation key/fingerprint into a metric label.
- [x] **Step 5: Verify:**

```bash
cargo test -p ukiel-catalog
cargo test -p ukiel-e2e --bin bench catalog
cargo clippy -p ukiel-catalog -p ukiel-e2e --all-targets -- -D warnings
```

- [x] **Step 6: Commit:**

```bash
git commit -m "feat: reconcile catalog commits by fingerprinted operation identity"
```

---

### Task 4: Give ingest commits deterministic identities

**Files:**
- Modify: `crates/ukiel-ingest/src/flusher.rs`
- Modify: `crates/ukiel-ingest/src/error.rs` only if identity propagation needs
  an explicit role-error accessor
- Modify: `crates/ukiel-ingest/tests/consumer_test.rs`
- Modify: catalog/e2e ingest fixtures affected by the required identity

Construct the ingest identity from the exact sorted offset ranges before any
upload. The operation's transformation-version constant lives beside the
flusher and changes when the same Kafka values would be mapped differently.
Generated part paths and Parquet byte layout are physical attempts and stay out
of identity. Pass the identity into `commit_with_offsets`; if final commit
acknowledgement is lost, propagate the typed `AmbiguousMutation` unchanged.

- [x] **Step 1: Failing identity tests.** Stable across offset input ordering
  and random output paths; different topic/partition/range/version differs.
- [x] **Step 2: Replay test.** Commit one offset range, replay the same identity
  with different pending output paths, receive the original commit ID, advance
  offsets once, expose only the first outputs, and leave losing uploads
  pending/orphan-cleanable.
- [x] **Step 3: Preserve offset-race semantics.** A genuinely different
  operation that overlaps an already-advanced range remains `OffsetRace`, not
  `AlreadyApplied` and not a transport retry.
- [x] **Step 4: Implement and verify:**

```bash
cargo test -p ukiel-ingest
cargo test -p ukiel-catalog --test catalog_test commit_with_offsets
cargo clippy -p ukiel-ingest --all-targets -- -D warnings
```

- [x] **Step 5: Commit:**

```bash
git commit -m "feat: identify ingest commits by Kafka offset intent"
```

---

### Task 5: Give compaction and deletion stable identities

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/src/deletion.rs`
- Modify: `crates/ukiel-compactor/src/error.rs` only if needed for typed
  identity propagation
- Modify: `crates/ukiel-compactor/tests/compactor_test.rs`
- Modify: `crates/ukiel-compactor/tests/deletion_test.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/catalog_load.rs`

Build the compaction identity as soon as the live input group and output level
are fixed, before opening Parquet. Build delete-key identity after the
authoritative overlapping-part read and before rewriting. Include transformation
inputs named above; exclude output `PartMeta` paths/size/statistics because they
may differ between physical attempts. The catalog still validates the submitted
part mutation normally on the first application.

- [x] **Step 1: Compaction identity/replay tests.** Same inputs with shuffled
  enumeration and different output UUIDs reconcile to one commit. Different
  level, input set, schema, placement, or partition is different intent.
- [x] **Step 2: Lease-order regression.** Lose the acknowledgement, release the
  lease, reacquire under the same process owner with a higher generation, then
  replay the old identity: return the durable commit before judging the stale
  lease. An absent old identity cannot pass the new lease fence.
- [x] **Step 3: Deletion tests.** Same key/input intent deduplicates different
  rewrite paths; a changed target key or input set does not. Dedicated-only and
  packed rewrites retain atomic all-or-none visibility.
- [x] **Step 4: Counting-store assertion.** Reconciliation never turns an
  already committed compaction into another Parquet read/upload. Losing pending
  objects remain eligible for ordinary GC.
- [x] **Step 5: Implement and verify:**

```bash
cargo test -p ukiel-compactor
cargo test -p ukiel-e2e --bin bench catalog
cargo clippy -p ukiel-compactor -p ukiel-e2e --all-targets -- -D warnings
```

- [x] **Step 6: Commit:**

```bash
git commit -m "feat: identify compaction and deletion mutations"
```

---

### Task 6: Reconcile ambiguous operations in the Plan 42 supervisor

**Files:**
- Modify: `crates/ukield/src/supervisor.rs`
- Modify: `crates/ukield/src/recovery.rs`
- Modify: `crates/ukield/src/health.rs` only if tests expose a transition gap
- Modify: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/metrics.rs` as needed

Extend `RoleFailure` with typed ambiguous-operation extraction. Preserve at most
one identity from the failed worker attempt. After catalog reachability returns,
lookup runs while the role is `Reconciling`; a lookup transport failure reuses
the existing bounded jitter policy and never rebuilds early. A collision is
permanent and fails the role. `Committed` and `Absent` are logged and counted,
then discarded before a fresh worker is built.

Required metrics:

- `catalog_mutation_reconciliation_total{role,outcome}` with bounded outcomes
  `committed`, `absent`, `collision`, `error`;
- `catalog_mutation_reconciliation_duration_seconds{role}`;
- retain `catalog_ambiguous_mutation_total{role}` and
  `compactor_ambiguous_commit_total` as observation counters during transition.

No key, fingerprint, hypertable, topic, partition value, or commit ID is a
metric label. Structured logs may include domain, hypertable ID, bounded key,
and authoritative commit ID.

- [x] **Step 1: Supervisor state-machine tests.** Ambiguous→Degraded→Reconciling
  →lookup committed/absent→rebuild→Healthy; lookup outage stays Reconciling;
  fingerprint collision becomes Failed; shutdown during lookup becomes Stopped.
- [x] **Step 2: Prove stale state is not carried.** The rebuilt ingest worker
  reloads offsets and re-seeks; compactor drops group/output/lease and replans
  current live parts even when lookup says absent.
- [x] **Step 3: Implement bounded lookup and observability.** Do not add a
  separate retry configuration or a second health state.
- [x] **Step 4: Verify:**

```bash
cargo test -p ukield supervisor
cargo test -p ukield recovery
cargo test -p ukield health
cargo clippy -p ukield --all-targets -- -D warnings
```

- [x] **Step 5: Commit:**

```bash
git commit -m "feat: reconcile ambiguous mutations before worker recovery"
```

---

### Task 7: Prove lost-ack convergence with deterministic fault injection

**Files:**
- Modify: `crates/ukiel-catalog/Cargo.toml`
- Add test-only fault seam in `crates/ukiel-catalog/src/commit.rs` or a dedicated
  `test-util` module; no default-build runtime knob
- Modify: `crates/ukiel-e2e/Cargo.toml`
- Create: `crates/ukiel-e2e/tests/s11_ambiguous_mutation.rs`
- Modify: `crates/ukiel-e2e/tests/common/mod.rs`
- Modify: `crates/ukiel-e2e/src/lib.rs`
- Modify: `Makefile`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`

Add an instance-scoped, feature-gated test seam with two one-shot points:
`before_commit` (operation definitely absent) and `after_commit_before_return`
(transaction durable, caller receives a synthetic transport-class ambiguous
error). It must not be configured by environment variables, global mutable
state, or production TOML, and must compile out of ordinary builds.

S11 combines the exact boundary seam with Plan 42's Toxiproxy writer outage:

1. ingest after-commit acknowledgement loss, catalog outage, restore, lookup
   committed, worker rebuild, and every Kafka event visible exactly once;
2. ingest before-commit failure, lookup absent, rebuild/replay, and exactly-once
   visibility;
3. compaction after-commit acknowledgement loss followed by lease release/
   expiry and peer handoff, lookup committed before lease judgement, no second
   object read/upload, and query equivalence;
4. compaction before-commit failure, lookup absent, fresh live-state replan and
   convergence;
5. same key/different fingerprint injection fails permanently and mutates
   neither parts nor offsets.

Observe role transitions and reconciliation metrics, not sleeps alone. Use
bounded polling deadlines derived from the existing retry policy. Reserve S11
for this plan; the stale Plan 8 MV scenario must be renumbered during its refresh.

- [x] **Step 1: Unit-test the test seam.** One shot, instance scoped, exact
  before/after semantics, absent from default feature set.
- [x] **Step 2: Implement S11 and run repeatedly.** At least 8 consecutive runs
  to catch handshake/failover races as S10 did.
- [x] **Step 3: Verify:**

```bash
cargo test --workspace
make e2e-ha
for i in $(seq 1 8); do cargo test -p ukiel-e2e --test s11_ambiguous_mutation -- --ignored; done
```

- [x] **Step 4: Commit:**

```bash
git commit -m "test: prove ambiguous catalog mutations converge exactly once"
```

---

### Task 8: Close the HA phase and hand off the next evidence gate

**Files:**
- Modify: `docs/issues/0013-compaction-lease-generation-resets-after-release.md`
- Regenerate: `docs/issues/README.md` with `docs/issues/generate-index.sh`
- Modify: `docs/superpowers/specs/2026-07-13-ukiel-high-availability.md`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan's status/checklists

- [x] **Step 1: Full static verification:**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

- [x] **Step 2: Mark issue 0013 resolved** with the sequence migration and ABA
  regression evidence, then regenerate the issue index.
- [x] **Step 3: Mark HA Phase 2 delivered.** Record the exact operation identity,
  lookup, supervisor transition, S11 evidence, and remaining Phase 3/4 gates.
  Do not claim horizontal ingest, GC singleton ownership, or Aurora RTO.
- [x] **Step 4: Mark roadmap row 43 executed.** Keep issue 0011's real million-
  logical-table proof as the next sequential evidence gate. Keep Plan 44
  independent/parallel. Mark Plan 8 as requiring a refresh onto Plan 42/43 APIs
  and a scenario number after S11 before execution.
- [x] **Step 5: Commit:**

```bash
git commit -m "docs: close ambiguous mutation HA phase"
```

## Acceptance criteria

Plan 43 is complete only when all of the following are true:

1. A clean release/reacquire never reuses a compaction lease generation, even
   for the same process owner, and every stale-token operation is rejected.
2. Operation identities are deterministic, versioned, domain-separated,
   bounded in length, and independent of generated object paths and attempts.
3. Key reuse with a different or unknowable fingerprint fails permanently and
   cannot return `AlreadyApplied`.
4. Ingest offset advancement and part visibility remain atomic with operation
   identity; compaction retains lease-before-object-work and transactional
   fencing.
5. A replay of committed leased work is reconciled before lease validation.
6. Only final commit-boundary transport failures are represented as ambiguous;
   semantic failures retain their existing classifications.
7. Plan 42's supervisor preserves the ambiguous identity, stays
   `Reconciling` through authoritative lookup, and rebuilds workers from
   catalog/Kafka/live-state authority after either committed or absent.
8. Direct callers have a typed lookup API and no generic unbounded SQL retry is
   introduced.
9. Ingest and compaction lost-ack tests prove committed and absent outcomes,
   exactly-once visibility, no stale lease success, and no duplicate compaction
   object work.
10. Production active-active compaction's ambiguity gate is closed explicitly;
    unrelated HA Phase 3/4 work remains unclaimed.

## Implementation cautions

1. **Do not fingerprint serialized `CommitOp`.** It contains generated output
   paths for ADD/REPLACE and would turn the same logical retry into a different
   operation.
2. **Do not infer commit outcome from object existence.** Uploaded objects
   precede the transaction and may be orphans; only the catalog lookup answers.
3. **Do not backfill unknown fingerprints.** A made-up digest converts unknown
   history into false certainty.
4. **Do not let `Absent` resume the old future.** Rebuild from offsets/live
   parts; otherwise reconciliation merely blesses stale state.
5. **Do not judge a replay's expired lease first.** If the commit already
   landed, `AlreadyApplied` is the authoritative result.
6. **Do not put operation identity in metric cardinality.** Log it in bounded
   structured fields when investigation needs it.
7. **Do not weaken optimistic conflict or offset CAS.** Idempotency answers
   duplicate intent; it does not replace semantic concurrency control.
8. **Do not expose the test seam in production configuration.** Exact lost-ack
   injection is a test facility, not an operator feature.
