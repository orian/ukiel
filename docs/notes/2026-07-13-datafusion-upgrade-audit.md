# DataFusion upgrade audit (roadmap row 38) — nothing to upgrade to yet

Audited 2026-07-13, for row 38 ("needs a release audit first"). **Finding: the
audit closes with no plan.** `datafusion 54.0.0` — ukiel's pin — is the newest
version on crates.io (published 2026-06-08; prior: 53.1.0 on 2026-04-16). A
plan written against an unreleased 55 would guess interfaces, which is the one
thing the plan convention exists to prevent. Row 38 therefore sits **blocked
upstream**, and this note is the standing recipe for whoever writes the plan
when the trigger fires.

## Already in 54 — sort-pushdown Phases 1–3 (corrections, 2026-07-13)

The sort-pushdown epic is [apache/datafusion#23036] ("skip sorts and skip
IO / decode for ORDER BY / TopK queries") — a phased initiative whose first
three phases are **in our pin**, not upstream-future:

| phase | released in | what it does |
|---|---|---|
| 1 — [PR #19064] | DF **52** | `PushdownSort` optimizer rule (`enable_sort_pushdown` default **true**, `datafusion-common-54.0.0/src/config.rs:1289`), `try_pushdown_sort` hooks on `DataSource`/`FileSource`, reverse file/row-group iteration (Inexact: `SortExec` stays, TopK terminates early) |
| 2 — PR #21182 | DF **53** | **statistics-based file reordering** for ORDER BY, runtime `DynamicFilter` pruning from the TopK heap (upstream: 27× on `ORDER BY LIMIT`) |
| 3 — PR #21956 | DF **54** | multi-partition support (`BufferExec`) |
| 4 — PR #22450 | DF **55**, in review | runtime row-group early stopping |

Verified live end-to-end in the vendored 54 sources: the machinery lives in
`datafusion-datasource-54.0.0/src/file_scan_config/sort_pushdown.rs`;
`FileScanConfig::try_pushdown_sort` (`file_scan_config/mod.rs:956`) delegates
to the file source **passing `self.eq_properties()`** — i.e. the provider's
declared output ordering is a *direct input* to the decision — and
`ParquetSource` implements the hook
(`datafusion-datasource-parquet-54.0.0/src/source.rs:830`), first checking
whether the natural ordering already satisfies the requested sort. Outcomes:
Exact (drop the `SortExec`), Inexact (keep it; reorder files/row groups by
statistics; TopK dynamic filters prune at runtime), Unsupported. **None of
this shows in EXPLAIN output.** Open arrow-rs dependencies for the DESC exact
path: arrow-rs#9937 (page-level reverse decode) and arrow-rs#10158
(`peek_next_row_group`). Consequences, neither belonging to row 38:

- **Row 37**: this is the leading *named candidate mechanism* for the
  declared-ordering effects we measured and could not see in plans — declared
  ordering → eq_properties → `ParquetSource::try_pushdown_sort` →
  file/row-group reordering + runtime dynamic filters, on by default,
  invisible in EXPLAIN. Plan 37's H5 carries it plus the one-line lever
  (`datafusion.optimizer.enable_sort_pushdown = false`, same binary).
- **Opportunity on 54, no upgrade needed**: ukiel's provider already benefits
  passively wherever it declares an ordering (predicated scans, post
  predicate-gate); the *active* opportunity — catalog-aware ordering claims so
  ORDER-BY-ts TopK over ts-sliced/dedicated files hits the Exact/Inexact
  paths — is implementable against 54's hooks. If row 37's attribution or the
  product suite motivates it, that is a new row — not a reason to hold row 38
  open.

## Watch triggers (any one re-opens the row)

1. **A DataFusion release newer than 54.0.0 on crates.io** — check
   `https://crates.io/api/v1/crates/datafusion` (`max_stable_version`).
2. **Sort-pushdown Phase 4 landing**: PR #22450 (runtime row-group early
   stopping) is in review targeting **DF 55** — the first concrete release
   target worth auditing; the DESC exact path additionally waits on
   arrow-rs#9937/#10158 (epic [apache/datafusion#23036]). Also still relevant
   on any newer release: whether predicate pushdown rebuilding the scan stops
   clearing the declared output ordering (plan-11 sort-elimination block; the
   plan-32-era predicate-gate workaround in `provider.rs`).
3. **Row 37's H5 disposition** naming a DF code path fixed in a newer release.

## What the eventual plan must contain (pre-written checklist)

- **Pin set moves in lockstep** — from the workspace `Cargo.toml`: `datafusion`
  and `datafusion-datasource-parquet` (same version, facade-unified — the
  comment at `Cargo.toml:27–30` explains why a drifting pin would break),
  `arrow`/`parquet` (currently 58.3), `object_store` (currently 0.13) at
  whatever versions the new DF links against — never independently (roadmap
  cross-plan constraint). Check the new DF's MSRV (54 requires Rust 1.88;
  ours is ≥ 1.96 — re-check both directions).
- **API surface to re-verify per crate** (grep-derived 2026-07-13; re-derive
  at plan time — plan 8's `ukiel-pipeline` will add a consumer):
  - `ukiel-query`: the deep end. `TableProvider::scan` + `FileScanConfigBuilder`
    + `ParquetSource` (+ predicate/pushdown/reorder flags), `PartitionedFile`
    (+ `with_extension`/`ParquetAccessPlan` — plan 16), `LexOrdering`/
    `PhysicalSortExpr`, `FilterExec`/`ProjectionExec`/`GlobalLimitExec`
    plan assembly, `SQLOptions`, and **`CachingParquetFileReaderFactory`**
    (`metadata_cache.rs` implements `AsyncFileReader` + a
    `ParquetFileReaderFactory` — historically the most churn-prone trait pair).
  - `ukiel-core`/`ukiel-ingest`/`ukiel-compactor`: direct `arrow`/`parquet`
    writers (`writer_props`, `sorting_columns`, `ArrowWriter`/
    `AsyncArrowWriter<ParquetObjectWriter>` streaming — plan 29,
    `flushed_row_groups()` pre-close — plan 16), `object_store` traits
    (`MultipartUpload` tee, `LocalFileSystem`, `AmazonS3Builder` — plan 15's
    cache implements `ObjectStore` end to end).
  - `ukiel-e2e` bench: raw-reference `SessionConfig` parquet options (the
    reference must keep matching the provider's reader settings — the plan-32
    confounder lesson), plan-37 instruments (`create_physical_plan`,
    `DataSourceExec::data_source()` downcasts, `Metric::partition()`).
- **Behavior re-validation, not just compilation** (each was measured on 54
  and may flip):
  1. The **predicate-gated ordering declaration** (`provider.rs`): re-run the
     gap-note same-binary A/B on the new version — if pushdown and declared
     ordering coexist, restore the unconditional declaration, delete the gate,
     and re-pin `order_by_sort_key_results_stay_correct_and_ordered` (its
     comment names this exact revisit).
  2. **Sort elimination** (plan-11 follow-up): does `ORDER BY ts` under
     `key = const` finally elide the `SortExec`? Update the perf note either way.
  3. **Plan-16 access plans**: `with_extension` mechanism and
     reduce-never-widen semantics re-verified in the new vendored source.
  4. **File-group repartitioning**: preserve-order behavior
     (`FileGroupPartitioner`) re-checked — row 37's F1, if it landed, may be
     removable or may need retuning.
- **Baselines re-anchored** — the recorded numbers encode engine behavior:
  re-run `bench hits queries`, `clickbench run` + `clickbench raw` (the
  reference moves with the engine — ratios must be recomputed, not reused),
  and the plan-34 ladder's DataFusion rungs. Label with the new version;
  never overwrite (append-only results convention).
- **Sequencing**: after plan 8 (pipelines adds the last big DF consumer —
  upgrade once, not twice), and coordinate with row 37 (its instruments make
  the upgrade's perf effects visible; its fixes may be superseded).

## Sizing expectation

If the release notes show only additive change on our surface: one plan,
~5 tasks (pins + per-crate fix-ups → behavior re-validation → baseline re-runs
→ docs). If `AsyncFileReader`/`ObjectStore`/writer traits break: consider
splitting read-side and write-side into two rows rather than one mega-plan.

[apache/datafusion#23036]: https://github.com/apache/datafusion/issues/23036
[PR #19064]: https://github.com/apache/datafusion/pull/19064
