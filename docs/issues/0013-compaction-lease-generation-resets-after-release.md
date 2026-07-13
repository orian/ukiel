# 0013 — Compaction lease generations reset after a clean release

- **Severity:** Medium (fencing-invariant hardening; difficult to trigger in the current sequential compactor)
- **Status:** Resolved (plan 43, task 1) — migration `0010_compaction_lease_generation_seq.sql` draws every new tenancy's generation from a catalog-wide sequence, so a released token is never reissued
- **Components:** `crates/ukiel-catalog`, `crates/ukiel-compactor`
- **Found by:** post-implementation review of plan 41, 2026-07-13

## Summary

Plan 41 defines each lease row as carrying a monotonically increasing generation.
The generation distinguishes a current tenancy from a stale token held by the
same process owner: renewal preserves it, while acquisition or reclamation starts
a new tenancy.

Expiry takeover implements that rule, but clean release does not. Release deletes
the row. The next acquisition inserts a new row with generation 1, and the catalog
test explicitly asserts that reset.

The resulting sequence is:

```text
owner A, generation 1 -> release/delete -> owner A, generation 1
```

An old token and the new token can therefore have the same partition, owner, and
generation. Those are exactly the fields used by renew, release, and the fenced
REPLACE transaction.

## Impact

The production compactor currently awaits the merge and renewal task before it
releases a lease, performs one merge per partition per pass, and does not run
concurrent partition jobs under one owner. That lifecycle makes an ABA collision
difficult to reach today.

It is still a broken fencing invariant and becomes reachable if later work adds:

- bounded in-process compaction concurrency;
- delayed cleanup/release tasks;
- reconciliation retaining an old lease token;
- another caller of the public catalog lease API.

In such a lifecycle, a stale token could renew or release a later tenancy owned
by the same process, or pass the lease portion of a REPLACE fence. Optimistic
part conflict checking still protects most current mutations, but the scheduling
fence should not rely on the caller never retaining an old token.

## Proposed resolution

Keep release immediate without reusing fencing tokens. The preferred design is a
PostgreSQL sequence used as the generation source:

1. Create a catalog-owned sequence for compaction lease generations.
2. Use `nextval` for a new row and for every expired/released takeover; preserve
   the generation only for an idempotent reacquisition by the same unexpired
   owner.
3. Continue deleting on clean release so the lease table and the "oldest expired
   lease" gauge remain bounded to active/abandoned work.
4. During migration, initialize the sequence above every existing generation.

Keeping one inactive row per partition is also correct if release becomes an
update, but it would make the lease table permanent history and would require an
explicit inactive state so clean releases do not pollute the abandoned-lease
gauge. A globally monotonic sequence is simpler; gaps are harmless because the
token needs uniqueness and ordering, not density.

## Acceptance criteria

- Releasing and reacquiring the same partition yields a generation greater than
  the released generation for both the same owner and a different owner.
- A stale pre-release token cannot renew, release, or commit against the new
  tenancy, including when the process owner UUID is unchanged.
- Idempotently reacquiring an unexpired lease by the current owner preserves its
  generation and does not invalidate an in-flight merge.
- Expiry takeover and concurrent acquisition retain the one-winner property.
- Clean release still makes the partition immediately claimable and does not
  create false abandoned-lease age.

## Fix

Migration `0010` creates `compaction_lease_generation_seq`. Every new tenancy —
a fresh row, or the takeover of an expired or released one — draws `nextval`; an
idempotent reacquisition by the same *unexpired* owner keeps its generation, so
an in-flight merge's fence survives its own retry. Clean release still deletes
the row, so the lease table and the abandoned-lease gauge stay bounded to
active/abandoned work. The sequence is initialized strictly above every
generation already present, so an upgrade cannot reissue a live tenancy.

Gaps are expected and harmless: a losing `INSERT ... ON CONFLICT` contender still
evaluates the VALUES list and burns a value it never uses. The token needs
uniqueness and ordering, not density — and the tests that asserted the *distance*
between generations now assert their *order*.

The regression test reproduces the ABA on the old counter (`1 -> 1` after a
release by the same owner) and pins that a pre-release token can neither renew,
release, nor pass the REPLACE fence against its successor. Plan 43's
reconciliation lifecycle — which carries an operation across a worker rebuild —
is exactly the caller that would have made this reachable, which is why it landed
first.

## Notes

- The process-instance UUID remains valuable: it identifies ownership in logs
  and blocks tokens from other processes. It does not replace a non-reused
  tenancy token within one process instance.
- This should land before any bounded in-process compaction concurrency or
  broader use of the lease API.
