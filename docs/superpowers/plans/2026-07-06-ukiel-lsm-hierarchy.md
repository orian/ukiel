# Ukiel Plan 17: LSM Level Hierarchy & Size-Targeted Placement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the binary placement policy and single-shot L0→L1 compaction with (a) a third placement, `SizeTargeted(bytes)` — merge outputs cut at packing-key boundaries into ~target-size files so heavy keys separate organically, in proportion to their size — and (b) a fanout-driven level ladder over runs (a run = one commit's key-disjoint output set) with a **two-tier trigger** — eager `l0_fanout` for level 0, lazier `fanout` for L1+ — and a cold-partition finalization pass that folds every quiet partition into a single key-disjoint run.

**Architecture:** `Placement` becomes a 3-variant enum stored as one nullable BIGINT (`hypertables.target_file_bytes`: NULL = packed, 0 = separated, N = size-targeted; size-targeted subsumes both extremes — packed = ∞, separated = 0). Run identity is derived from the existing `created_by_commit` column — parts committed together form one key-disjoint run, so **no parts-table change**. The compactor gains: per-(partition, level) run counting with a two-tier fanout trigger replacing `min_l0_files` — a low `l0_fanout` for level 0 and a higher `fanout` for L1+, because the levels are asymmetric: L0 files span ~all tenants (no key pruning applies, read amp is linear in their count, and they're tiny so merges are cheap), while L1+ runs are internally key-disjoint (a query touches ≤1 file per run) and their merges move real bytes — a generalized `merge_parts(parts, output_level)` (replaces `merge_group`), chunk planning in `rewrite::plan_chunks` (replaces the placement `match` in the compactor), and `finalize_once` on a second, slower ticker (finalization cannot be gated on the change feed — a cold partition emits no events). Query path, GC, key deletion, ingest, and the commit protocol are untouched (`delete_key` already branches on part *shape*, not placement).

The model this implements (runs, the coverage-inversion at the size-target crossover, read-amp bound fanout × log_fanout(N) hot → ~1 file per cold partition) is recorded in the design doc's "Placement policy" section and gets its own concept note in Task 7.

**Tech Stack:** Rust edition 2024, sqlx (Postgres, migrations in `crates/ukiel-catalog/migrations/`), arrow/parquet 58.3 (pinned), testcontainers component tests.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (`cargo test -- --test-threads=2`, Docker required); unit tests added by Tasks 1 and 3 must pass without Docker (`cargo test -p <crate> --lib`).
- **Every existing catalog, ingest, compactor, query, gc, and ukield test stays green after every task.** `CompactorConfig::default()` sets `l0_fanout = 2, fanout = 2`, deliberately preserving the old `min_l0_files = 2` behavior so existing tests keep passing unmodified. The operator-facing `[compactor]` section defaults to the production shape instead (`l0_fanout = 4`, `fanout = 10`) — this divergence is intentional (library default = test compatibility, server default = recommended tuning).
- **Plan 11/14 adaptation:** as of writing (2026-07-06), `rewrite::batch_to_parquet(batch)` takes one argument and builds plain ZSTD props. If plan 14 (`2026-07-06-ukiel-file-layout.md`) lands first, it becomes `batch_to_parquet(batch, props)` plus `batch_to_parquet_key_aligned` — adapt Task 4's call sites to the new signatures (`plan_chunks` is upstream of encoding and unaffected); use the key-aligned variant for any chunk whose `key_min != key_max`.
- **Plan 12 adaptation:** plan 12 (`2026-07-06-ukiel-catalog-stats-pruning.md`) makes writers populate `PartMeta.column_stats`. Task 4's `merge_parts` sets `column_stats: None`, matching today's writer. If plan 12 lands first, populate stats in `merge_parts` exactly as plan 12's compactor task does — executing this plan verbatim would **silently regress** catalog stats pruning (no test fails; pruning quality just degrades). The roadmap's recommended order (17 before 12) avoids this.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: 3-variant `Placement` in ukiel-core

**Files:**
- Modify: `crates/ukiel-core/src/table.rs`

**Interfaces:**
- Produces: `Placement::{Packed, Separated, SizeTargeted(i64)}` with `from_db(Option<i64>) -> Placement` and `to_db(&self) -> Option<i64>`. `as_str()` is removed (its only caller, `set_placement`, switches to `to_db` in Task 2). `Hypertable.placement` keeps its name and type. No Docker needed.

- [ ] **Step 1: Write the failing tests**

Append at the bottom of `crates/ukiel-core/src/table.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_db_round_trip() {
        assert_eq!(Placement::from_db(None), Placement::Packed);
        assert_eq!(Placement::from_db(Some(0)), Placement::Separated);
        assert_eq!(
            Placement::from_db(Some(268_435_456)),
            Placement::SizeTargeted(268_435_456)
        );
        // Defensive: non-positive targets mean "every key alone".
        assert_eq!(Placement::from_db(Some(-5)), Placement::Separated);

        for p in [
            Placement::Packed,
            Placement::Separated,
            Placement::SizeTargeted(1024),
        ] {
            assert_eq!(Placement::from_db(p.to_db()), p);
        }
        assert_eq!(Placement::Packed.to_db(), None);
        assert_eq!(Placement::Separated.to_db(), Some(0));
        assert_eq!(Placement::SizeTargeted(7).to_db(), Some(7));
    }
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-core --lib placement_db_round_trip`
Expected: compile error (`SizeTargeted`, `from_db`, `to_db` don't exist).

- [ ] **Step 3: Implement**

In `crates/ukiel-core/src/table.rs`, replace the whole `Placement` enum and its `impl` (currently lines 5–23) with:

```rust
/// Where a hypertable's data lives at rest (L1+). L0 is always packed.
/// Stored as one nullable BIGINT (`hypertables.target_file_bytes`):
/// NULL = packed, 0 = separated, N > 0 = size-targeted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    /// Each merge output is one packed file, sorted by packing key (default).
    Packed,
    /// Every packing key gets dedicated files; key deletion and retention
    /// become metadata-only at rest.
    Separated,
    /// Merge outputs are cut at key boundaries into files of roughly this
    /// many bytes: small keys share files, a key bigger than the target gets
    /// files of its own (`min == max`) — heavy keys separate organically,
    /// in proportion to their size.
    SizeTargeted(i64),
}

impl Placement {
    pub fn from_db(target_file_bytes: Option<i64>) -> Self {
        match target_file_bytes {
            None => Placement::Packed,
            Some(n) if n <= 0 => Placement::Separated,
            Some(n) => Placement::SizeTargeted(n),
        }
    }

    pub fn to_db(&self) -> Option<i64> {
        match self {
            Placement::Packed => None,
            Placement::Separated => Some(0),
            Placement::SizeTargeted(n) => Some(*n),
        }
    }
}
```

- [ ] **Step 4: Verify core passes; survey downstream breakage**

Run: `cargo test -p ukiel-core --lib`
Expected: PASS.

Run: `cargo build --workspace`
Expected: FAILS in `ukiel-catalog` (`as_str` gone — Task 2's job). That is the only expected breakage. If a red intermediate build is undesirable, implement Task 2 before committing and commit Tasks 1+2 together.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-core --all-targets -- -D warnings
git add crates/ukiel-core
git commit -m "feat: placement gains a size-targeted variant stored as target_file_bytes"
```

---

### Task 2: Catalog — migration, placement round-trip, L0-quiet probe

**Files:**
- Create: `crates/ukiel-catalog/migrations/0005_target_file_bytes.sql`
- Modify: `crates/ukiel-catalog/src/tables.rs`
- Modify: `crates/ukiel-catalog/src/parts.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Consumes: `Placement::{from_db, to_db}` (Task 1).
- Produces: `set_placement(id, Placement)` (same name, now handles all 3 variants); `partition_l0_quiet_since(hypertable_id, partition_values: &serde_json::Value, quiet_secs: f64) -> Result<bool, CatalogError>` — true iff no L0 part (live, tombstoned, or purged — catalog rows are never deleted) was committed to that partition within the last `quiet_secs`. Docker required for tests.

- [ ] **Step 1: Write the migration**

Create `crates/ukiel-catalog/migrations/0005_target_file_bytes.sql`:

```sql
-- Placement generalized to one knob (spec: size-targeted placement):
-- NULL = packed (one file per merge output),
-- 0 = separated (every packing key its own files),
-- N > 0 = size-targeted (merge outputs cut at key boundaries into ~N-byte
-- files; keys bigger than N get dedicated files -> heavy keys separate
-- organically).
ALTER TABLE hypertables
    ADD COLUMN target_file_bytes BIGINT
    CHECK (target_file_bytes IS NULL OR target_file_bytes >= 0);

UPDATE hypertables SET target_file_bytes = 0 WHERE placement = 'separated';

ALTER TABLE hypertables DROP COLUMN placement;
```

- [ ] **Step 2: Write the failing tests**

In `crates/ukiel-catalog/tests/catalog_test.rs`, append (reuse the file's existing setup helpers for a catalog + hypertable; adapt the helper names to the file's actual ones — it already creates hypertables and commits `PartMeta`s in every test; `add_l0_part` below = one `CommitOp::Add` of a `PartMeta { level: 0, partition_values: partition.clone(), .. }` with any path/rows):

```rust
#[tokio::test]
async fn placement_round_trips_all_variants() {
    let env = setup().await;

    for placement in [
        ukiel_core::Placement::SizeTargeted(268_435_456),
        ukiel_core::Placement::Separated,
        ukiel_core::Placement::Packed,
    ] {
        env.catalog.set_placement(env.ht, placement).await.unwrap();
        let ht = env.catalog.get_hypertable_by_id(env.ht).await.unwrap();
        assert_eq!(ht.placement, placement);
    }
}

#[tokio::test]
async fn partition_l0_quiet_since_tracks_l0_commits() {
    let env = setup().await;
    let partition = serde_json::json!({ "day": "2026-07-01" });

    // No L0 parts ever: quiet at any window.
    assert!(
        env.catalog
            .partition_l0_quiet_since(env.ht, &partition, 3600.0)
            .await
            .unwrap()
    );

    add_l0_part(&env, &partition).await;

    // Fresh L0: not quiet for a 1h window, quiet for a 0s window.
    assert!(
        !env.catalog
            .partition_l0_quiet_since(env.ht, &partition, 3600.0)
            .await
            .unwrap()
    );
    assert!(
        env.catalog
            .partition_l0_quiet_since(env.ht, &partition, 0.0)
            .await
            .unwrap()
    );

    // A different partition is unaffected.
    assert!(
        env.catalog
            .partition_l0_quiet_since(env.ht, &serde_json::json!({ "day": "2026-07-02" }), 3600.0)
            .await
            .unwrap()
    );
}
```

- [ ] **Step 3: Verify tests fail**

Run: `cargo test -p ukiel-catalog --test catalog_test placement_round_trips_all_variants`
Expected: compile error (old `set_placement` still binds `as_str`; `partition_l0_quiet_since` missing).

- [ ] **Step 4: Implement `tables.rs`**

In `crates/ukiel-catalog/src/tables.rs`:

1. In all three SELECT queries (`get_hypertable`, `get_hypertable_by_id`, `list_hypertables`), replace the column `placement` with `target_file_bytes`.
2. Add a private row-mapping helper and use it in all three (replacing the three duplicated `Hypertable { ... }` literals):

```rust
fn hypertable_from_row(row: &sqlx::postgres::PgRow) -> Hypertable {
    Hypertable {
        id: HypertableId(row.get("id")),
        name: row.get("name"),
        table_schema: row.get("table_schema"),
        partition_spec: row.get("partition_spec"),
        sort_key: row.get("sort_key"),
        packing_key: row.get("packing_key"),
        placement: Placement::from_db(row.get("target_file_bytes")),
    }
}
```

3. Replace `set_placement`'s body:

```rust
    pub async fn set_placement(
        &self,
        id: HypertableId,
        placement: Placement,
    ) -> Result<(), CatalogError> {
        sqlx::query("UPDATE hypertables SET target_file_bytes = $1 WHERE id = $2")
            .bind(placement.to_db())
            .bind(id.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
```

- [ ] **Step 5: Implement the quiet probe in `parts.rs`**

Append to the `impl PostgresCatalog` block in `crates/ukiel-catalog/src/parts.rs`:

```rust
    /// True iff no L0 part was committed to this partition within the last
    /// `quiet_secs` (rows are never deleted, so tombstoned/purged L0 parts
    /// still count as arrivals). Drives cold-partition finalization.
    pub async fn partition_l0_quiet_since(
        &self,
        hypertable_id: HypertableId,
        partition_values: &serde_json::Value,
        quiet_secs: f64,
    ) -> Result<bool, CatalogError> {
        let quiet: bool = sqlx::query_scalar(
            "SELECT coalesce(
                 max(c.created_at) < now() - make_interval(secs => $3),
                 true)
             FROM parts p JOIN commits c ON c.id = p.created_by_commit
             WHERE p.hypertable_id = $1 AND p.partition_values = $2
               AND p.level = 0",
        )
        .bind(hypertable_id.0)
        .bind(partition_values)
        .bind(quiet_secs)
        .fetch_one(&self.pool)
        .await?;
        Ok(quiet)
    }
```

- [ ] **Step 6: Verify pass**

Run: `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: the two new tests and all existing catalog tests PASS (migration applies cleanly; existing tests using `Placement::Separated` still compile — the variant survives).

Run: `cargo build --workspace`
Expected: green (`ukield` and `ukiel-compactor` compile because `set_placement(id, Placement::Separated)` keeps its signature).

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog crates/ukiel-core
git commit -m "feat: placement stored as target_file_bytes; partition L0-quiet probe"
```

---

### Task 3: `rewrite::plan_chunks` — placement-driven output planning

**Files:**
- Modify: `crates/ukiel-compactor/src/rewrite.rs`

**Interfaces:**
- Consumes: `Placement` (Task 1), existing `split_by_key` / `key_range`.
- Produces: `pub fn plan_chunks(sorted: &RecordBatch, packing_key: &str, placement: &Placement, bytes_per_row: f64) -> Result<Vec<(i64, i64, RecordBatch)>, CompactorError>` — key-disjoint, contiguous, order-preserving chunks; the single place placement semantics live. No Docker needed (`--lib`).

- [ ] **Step 1: Write the failing tests**

Append inside `mod tests` in `crates/ukiel-compactor/src/rewrite.rs`:

```rust
    /// (tenant_id, ts, payload) batch sorted by tenant: run lengths per key.
    fn sorted_batch(runs: &[(i64, usize)]) -> RecordBatch {
        let arrow_schema = Arc::new(arrow_schema_from_json(&schema_json()).unwrap());
        let mut tenants = Vec::new();
        let mut ts = Vec::new();
        for &(key, n) in runs {
            for i in 0..n {
                tenants.push(key);
                ts.push(i as i64);
            }
        }
        let payload: arrow::array::StringArray =
            tenants.iter().map(|k| Some(format!("p{k}"))).collect();
        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(tenants)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(payload),
            ],
        )
        .unwrap()
    }

    fn ranges_and_rows(chunks: &[(i64, i64, RecordBatch)]) -> Vec<(i64, i64, usize)> {
        chunks
            .iter()
            .map(|(min, max, b)| (*min, *max, b.num_rows()))
            .collect()
    }

    #[test]
    fn plan_chunks_packed_and_separated_match_old_semantics() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        let packed = plan_chunks(&batch, "tenant_id", &Placement::Packed, 10.0).unwrap();
        assert_eq!(ranges_and_rows(&packed), vec![(1, 4, 13)]);

        let separated = plan_chunks(&batch, "tenant_id", &Placement::Separated, 10.0).unwrap();
        assert_eq!(
            ranges_and_rows(&separated),
            vec![(1, 1, 3), (2, 2, 3), (3, 3, 6), (4, 4, 1)]
        );
    }

    #[test]
    fn plan_chunks_size_targeted_packs_small_keys_and_cuts_at_boundaries() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        // 10 bytes/row, 100-byte target: k1+k2 = 60 fits, +k3 would be 120
        // -> cut; k3+k4 = 70 fits.
        let chunks =
            plan_chunks(&batch, "tenant_id", &Placement::SizeTargeted(100), 10.0).unwrap();
        assert_eq!(ranges_and_rows(&chunks), vec![(1, 2, 6), (3, 4, 7)]);

        // Chunks are contiguous, ordered, disjoint, and lossless.
        let total: usize = chunks.iter().map(|(_, _, b)| b.num_rows()).sum();
        assert_eq!(total, batch.num_rows());
        for w in chunks.windows(2) {
            assert!(w[0].1 < w[1].0, "overlapping chunk ranges");
        }
    }

    #[test]
    fn plan_chunks_isolates_keys_bigger_than_the_target() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        // 40-byte target: every key run alone overflows any shared chunk;
        // the 60-byte whale (k3) still lands whole in its own chunk.
        let chunks =
            plan_chunks(&batch, "tenant_id", &Placement::SizeTargeted(40), 10.0).unwrap();
        assert_eq!(
            ranges_and_rows(&chunks),
            vec![(1, 1, 3), (2, 2, 3), (3, 3, 6), (4, 4, 1)]
        );
    }
```

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p ukiel-compactor --lib`
Expected: compile error (`plan_chunks` doesn't exist).

- [ ] **Step 3: Implement**

In `crates/ukiel-compactor/src/rewrite.rs`, add `use ukiel_core::Placement;` next to the `ukiel_core::Part` import, and insert after `key_range`:

```rust
/// Plans merge output chunks per the placement policy, over a batch already
/// sorted with `packing_key` as the primary sort column. Chunks are
/// contiguous slices in key order, so every output set is key-disjoint —
/// one merge's outputs form one "run". `bytes_per_row` is the caller's
/// encoded-size estimate (input parts' size/rows), used to hit
/// `SizeTargeted` before encoding; a key run bigger than the target stays
/// whole in its own chunk (`min == max`), never split mid-key.
pub fn plan_chunks(
    sorted: &RecordBatch,
    packing_key: &str,
    placement: &Placement,
    bytes_per_row: f64,
) -> Result<Vec<(i64, i64, RecordBatch)>, CompactorError> {
    match placement {
        Placement::Packed => {
            let (min, max) = key_range(sorted, packing_key)?;
            Ok(vec![(min, max, sorted.clone())])
        }
        Placement::Separated => Ok(split_by_key(sorted, packing_key)?
            .into_iter()
            .map(|(key, batch)| (key, key, batch))
            .collect()),
        Placement::SizeTargeted(target) => {
            let target = (*target).max(1) as f64;
            let bytes_per_row = bytes_per_row.max(1.0);
            let mut chunks = Vec::new();
            let mut start = 0usize; // first row of the open chunk
            let mut rows = 0usize; // rows in the open chunk
            let (mut min, mut max) = (0i64, 0i64);
            for (key, run) in split_by_key(sorted, packing_key)? {
                if rows > 0 && (rows + run.num_rows()) as f64 * bytes_per_row > target {
                    chunks.push((min, max, sorted.slice(start, rows)));
                    start += rows;
                    rows = 0;
                }
                if rows == 0 {
                    min = key;
                }
                max = key;
                rows += run.num_rows();
            }
            if rows > 0 {
                chunks.push((min, max, sorted.slice(start, rows)));
            }
            Ok(chunks)
        }
    }
}
```

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-compactor --lib`
Expected: all three new tests and `merge_sort_split_round_trip` PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-compactor --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: placement-driven chunk planning in the rewrite engine"
```

---

### Task 4: Compactor — fanout ladder over runs

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Consumes: `rewrite::plan_chunks` (Task 3), `Placement` (Task 1).
- Produces: `CompactorConfig { worker_name, l0_fanout: usize, fanout: usize, finalize_after_secs: u64, finalize_poll_interval_ms: u64, poll_interval_ms }` (**`min_l0_files` removed**; defaults `l0_fanout: 2, fanout: 2` preserve old behavior, `finalize_after_secs: 3600`, `finalize_poll_interval_ms: 60_000`); private `merge_parts(hypertable, parts, output_level)` (replaces `merge_group`); output paths `ht/{id}/L{output_level}/{uuid}.parquet`. `finalize_after_secs`/`finalize_poll_interval_ms` are consumed in Task 5 — adding them here keeps the config change in one commit. Docker required for tests.

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
#[tokio::test]
async fn compaction_builds_a_level_hierarchy() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(), // fanout = 2
    );

    // Two L0 runs -> one L1 run.
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})]).await;
    compactor.run_once().await.unwrap();
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.level, 1);

    // Two more L0 runs -> a second L1 run; then two L1 runs -> one L2 run.
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 4, "ts": 40, "payload": "d"})]).await;
    compactor.run_once().await.unwrap(); // merges the L0s (level 0 first)
    compactor.run_once().await.unwrap(); // now two L1 runs -> L2

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1, "cold ladder collapses to one part: {parts:?}");
    assert_eq!(parts[0].meta.level, 2);
    assert!(parts[0].meta.path.contains("/L2/"), "path: {}", parts[0].meta.path);
    assert_eq!(
        live_payloads(&env).await,
        ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn size_targeted_placement_isolates_heavy_keys() {
    let env = setup().await;

    // Whale tenant 42: 100 rows. Tenants 1-4: 2 rows each (8 rows total).
    let whale: Vec<_> = (0..100)
        .map(|i| json!({"tenant_id": 42, "ts": i, "payload": format!("w{i}")}))
        .collect();
    let small: Vec<_> = (1..=4i64)
        .flat_map(|t| {
            (0..2).map(move |i| json!({"tenant_id": t, "ts": i, "payload": format!("s{t}x{i}")}))
        })
        .collect();
    add_l0(&env, "2026-07-01", whale).await;
    add_l0(&env, "2026-07-01", small).await;

    // Target = ~20 rows' worth of bytes (from the real L0 stats): the four
    // small tenants (8 rows) share one chunk, the whale (100 rows)
    // overflows any shared chunk and gets its own file.
    let live = env.catalog.live_parts(env.ht, None).await.unwrap();
    let bytes: i64 = live.iter().map(|p| p.meta.size_bytes).sum();
    let rows: i64 = live.iter().map(|p| p.meta.row_count).sum();
    let target = (bytes as f64 / rows as f64 * 20.0) as i64;
    env.catalog
        .set_placement(env.ht, Placement::SizeTargeted(target))
        .await
        .unwrap();

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    compactor.run_once().await.unwrap();

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2, "smalls packed + whale isolated: {parts:?}");
    let whale_part = parts.iter().find(|p| p.meta.packing_key_min == 42).unwrap();
    assert_eq!(whale_part.meta.packing_key_max, 42, "whale file is min==max");
    assert_eq!(whale_part.meta.row_count, 100);
    let small_part = parts.iter().find(|p| p.meta.packing_key_min == 1).unwrap();
    assert_eq!(
        (small_part.meta.packing_key_min, small_part.meta.packing_key_max, small_part.meta.row_count),
        (1, 4, 8)
    );
}

#[tokio::test]
async fn l0_and_upper_levels_use_their_own_fanout() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            l0_fanout: 2,
            fanout: 3, // L1+ merges need three runs
            ..CompactorConfig::default()
        },
    );

    // Two rounds of two L0s: L0 merges eagerly at 2, the resulting L1 runs
    // accumulate below the lazier upper trigger.
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})]).await;
    compactor.run_once().await.unwrap();
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 4, "ts": 40, "payload": "d"})]).await;
    compactor.run_once().await.unwrap();

    let levels: Vec<i16> = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.meta.level)
        .collect();
    assert_eq!(levels, vec![1, 1], "two L1 runs wait below the L1+ fanout");
    assert_eq!(compactor.run_once().await.unwrap().merged_groups, 0);

    // A third L1 run reaches the upper trigger -> one L2 run.
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 5, "ts": 50, "payload": "e"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 6, "ts": 60, "payload": "f"})]).await;
    compactor.run_once().await.unwrap(); // -> third L1 run
    compactor.run_once().await.unwrap(); // 3 L1 runs -> L2

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.level, 2);
}
```

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p ukiel-compactor --test compactor_test`
Expected: compile error (`l0_fanout` doesn't exist yet). Behind it, the semantic failures the tests encode: the old compactor only ever looks at level 0, so `compaction_builds_a_level_hierarchy` would leave 2+ level-1 parts and never build an L2; `size_targeted...` would get one packed part covering 1–42; `l0_and_upper_levels...` would merge L1 runs at 2 instead of waiting for 3.

- [ ] **Step 3: Implement**

In `crates/ukiel-compactor/src/compactor.rs`:

1. Imports: change the `std::collections` line to `use std::collections::{BTreeMap, HashMap, HashSet};` and drop `Placement` from the `ukiel_core` import list (it's now only used inside `rewrite`).

2. Replace `CompactorConfig` and its `Default`:

```rust
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Cursor identity in `worker_cursors`.
    pub worker_name: String,
    /// Merge a partition's L0 files once at least this many exist. L0 files
    /// span ~all tenants — no key pruning applies, so every query reads
    /// every live L0 file: keep this low. L0 files are tiny; the merges
    /// are cheap.
    pub l0_fanout: usize,
    /// Run-count merge trigger for L1 and above. A run = the key-disjoint
    /// output of one commit (distinct `created_by_commit` among live
    /// parts); a query touches <= 1 file per run, so extra runs are cheap
    /// to leave lying around while their merges move real bytes — keep this
    /// higher than `l0_fanout`. Read amp per key <= l0_fanout + fanout x
    /// depth while a partition is hot.
    pub fanout: usize,
    /// A partition with no L0 arrivals for this long and more than one live
    /// run is folded into a single run one level above its maximum
    /// (finalization; see `finalize_once`).
    pub finalize_after_secs: u64,
    /// Cadence of the finalization sweep inside `run`.
    pub finalize_poll_interval_ms: u64,
    /// Loop tick for the merge pass in `run`.
    pub poll_interval_ms: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            worker_name: "compactor".to_string(),
            // Both 2: preserves the old `min_l0_files = 2` test behavior.
            // Production tuning lives in the ukield config defaults (4/10).
            l0_fanout: 2,
            fanout: 2,
            finalize_after_secs: 3600,
            finalize_poll_interval_ms: 60_000,
            poll_interval_ms: 5000,
        }
    }
}
```

3. In `compact_hypertable`, replace everything from `// Plan from live state...` down to (but not including) the `// Advance past everything we saw.` comment with:

```rust
        // Plan from live state, never from the events themselves.
        let live = self.catalog.live_parts(hypertable.id, None).await?;
        let mut partitions: HashMap<String, Vec<Part>> = HashMap::new();
        for part in live {
            partitions
                .entry(part.meta.partition_values.to_string())
                .or_default()
                .push(part);
        }

        for (_, parts) in partitions {
            // Runs per level: parts committed together form one key-disjoint
            // run, so distinct commits = distinct runs (each L0 file is its
            // own ingest commit).
            let mut by_level: BTreeMap<i16, Vec<Part>> = BTreeMap::new();
            for part in parts {
                by_level.entry(part.meta.level).or_default().push(part);
            }
            for (level, group) in by_level {
                let runs: HashSet<i64> =
                    group.iter().map(|p| p.created_by_commit.0).collect();
                // Two-tier trigger: L0 files are unpruned by key (merge
                // eagerly), L1+ runs are key-disjoint (merge lazily).
                let trigger = if level == 0 {
                    self.config.l0_fanout
                } else {
                    self.config.fanout
                };
                if runs.len() < trigger {
                    continue;
                }
                match self.merge_parts(hypertable, &group, level + 1).await {
                    Ok(parts_out) => {
                        stats.merged_groups += 1;
                        stats.parts_in += group.len();
                        stats.parts_out += parts_out;
                    }
                    // Another writer got here first: replan next pass.
                    Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {
                        stats.conflicts += 1;
                    }
                    Err(e) => return Err(e),
                }
                // One merge per partition per pass: bounded commits; the
                // ladder cascades over subsequent passes.
                break;
            }
        }
```

4. Replace `merge_group` entirely with:

```rust
    /// Merges the given live parts (all one partition) into one run at
    /// `output_level` per the placement policy and commits the swap. The
    /// run's files are key-disjoint by construction (`plan_chunks`).
    /// Returns the number of output parts.
    async fn merge_parts(
        &self,
        hypertable: &Hypertable,
        group: &[Part],
        output_level: i16,
    ) -> Result<usize, CompactorError> {
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let schema = Arc::new(cols.physical_schema());
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        // Rewrites recompute write-time columns: this is what backfills a
        // materialized column added after these files were written.
        let merged = ukiel_expr::apply_defaults_and_materialized(merged, &cols)?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
        let partition_values = group[0].meta.partition_values.clone();

        // Encoded-size estimate for SizeTargeted, from the inputs' stats.
        let total_bytes: i64 = group.iter().map(|p| p.meta.size_bytes).sum();
        let total_rows: i64 = group.iter().map(|p| p.meta.row_count).sum();
        let bytes_per_row = (total_bytes as f64 / total_rows.max(1) as f64).max(1.0);

        let outputs = rewrite::plan_chunks(
            &sorted,
            &hypertable.packing_key,
            &hypertable.placement,
            bytes_per_row,
        )?;

        let mut new_parts = Vec::with_capacity(outputs.len());
        for (key_min, key_max, batch) in &outputs {
            let bytes = rewrite::batch_to_parquet(batch)?;
            let path = format!(
                "ht/{}/L{}/{}.parquet",
                hypertable.id,
                output_level,
                uuid::Uuid::new_v4()
            );
            let size_bytes = bytes.len() as i64;
            self.store
                .put(&Path::from(path.clone()), bytes.into())
                .await?;
            new_parts.push(PartMeta {
                path,
                partition_values: partition_values.clone(),
                packing_key_min: *key_min,
                packing_key_max: *key_max,
                row_count: batch.num_rows() as i64,
                size_bytes,
                level: output_level,
                column_stats: None,
            });
        }

        let old = group.iter().map(|p| p.id).collect();
        let count = new_parts.len();
        // Conflict propagates as CatalogError::Conflict; uploaded objects are
        // then orphans — harmless, cleaned by GC's orphan sweep.
        self.catalog
            .commit(
                hypertable.id,
                CommitOp::Replace {
                    old,
                    new: new_parts,
                },
                None,
            )
            .await?;
        Ok(count)
    }
```

5. Update the module doc comment (line 1) to:

```rust
//! Fanout-driven level-ladder compaction. The change feed (via the durable
//! cursor) is only a wake-up signal; every pass plans from current
//! live-parts state, so passes are idempotent and REPLACE conflicts are
//! safely retried next pass. A run = one commit's key-disjoint output set;
//! `fanout` runs at a level merge into one run a level up.
```

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: both new tests PASS; all existing tests (packed, separated, races, deletion, backfill) stay green — default `fanout: 2` matches old `min_l0_files: 2`, packed/separated semantics are byte-identical via `plan_chunks`, `delete_key` is untouched.

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: green (readers are layout/level-agnostic).

Note: `ukield` does not compile yet (`min_l0_files` gone) — fixed in Task 6. If a fully-green workspace is required per commit, fold Task 6's `run.rs`/`config.rs` change into this commit.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-compactor --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: fanout-driven level ladder over commit-runs in the compactor"
```

---

### Task 5: Cold-partition finalization

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Consumes: `partition_l0_quiet_since` (Task 2), `merge_parts` (Task 4).
- Produces: `pub async fn finalize_once(&self) -> Result<usize, CompactorError>` (partitions finalized); `run` drives it on `finalize_poll_interval_ms`. Terminal invariant: a cold partition converges to exactly one run = a set of pairwise key-disjoint files. Docker required.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
#[tokio::test]
async fn cold_partitions_finalize_into_a_single_run() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            l0_fanout: 100,         // the normal ladder never triggers
            fanout: 100,
            finalize_after_secs: 0, // everything is instantly cold
            ..CompactorConfig::default()
        },
    );

    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})]).await;
    add_l0(&env, "2026-07-01", vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})]).await;

    // Below fanout: the merge pass does nothing.
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 0);

    // Finalization folds the cold partition into a single run.
    assert_eq!(compactor.finalize_once().await.unwrap(), 1);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1, "one packed run: {parts:?}");
    assert_eq!(parts[0].meta.level, 1); // max input level 0 + 1
    assert_eq!(
        live_payloads(&env).await,
        ["a", "b", "c"].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
    );

    // A single run is final: the sweep is idempotent.
    assert_eq!(compactor.finalize_once().await.unwrap(), 0);
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-compactor --test compactor_test cold_partitions_finalize`
Expected: compile error (`finalize_once` doesn't exist).

- [ ] **Step 3: Implement**

In `crates/ukiel-compactor/src/compactor.rs`:

1. Add after `run_once`:

```rust
    /// One finalization sweep: every partition with no L0 arrivals for
    /// `finalize_after_secs` and more than one live run is folded into a
    /// single run one level above its current maximum. After it, all of the
    /// partition's files are pairwise key-disjoint: one file chain per key,
    /// exact range pruning, metadata-only deletion for dedicated keys.
    /// Not gated on the change feed — a cold partition emits no events.
    /// Returns the number of partitions finalized.
    pub async fn finalize_once(&self) -> Result<usize, CompactorError> {
        let mut finalized = 0;
        for hypertable in self.catalog.list_hypertables().await? {
            let live = self.catalog.live_parts(hypertable.id, None).await?;
            let mut partitions: HashMap<String, Vec<Part>> = HashMap::new();
            for part in live {
                partitions
                    .entry(part.meta.partition_values.to_string())
                    .or_default()
                    .push(part);
            }
            for (_, parts) in partitions {
                let runs: HashSet<i64> =
                    parts.iter().map(|p| p.created_by_commit.0).collect();
                if runs.len() < 2 {
                    continue; // already one key-disjoint run: final
                }
                let quiet = self
                    .catalog
                    .partition_l0_quiet_since(
                        hypertable.id,
                        &parts[0].meta.partition_values,
                        self.config.finalize_after_secs as f64,
                    )
                    .await?;
                if !quiet {
                    continue;
                }
                let top = parts.iter().map(|p| p.meta.level).max().unwrap_or(0);
                match self.merge_parts(&hypertable, &parts, top + 1).await {
                    Ok(_) => finalized += 1,
                    // Raced by a merge/deletion: replan next sweep.
                    Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(finalized)
    }
```

2. Replace the loop body of `run` so both tickers drive the worker:

```rust
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut finalize_ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.finalize_poll_interval_ms,
        ));
        finalize_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let stats = self.run_once().await?;
                    if stats.merged_groups > 0 || stats.conflicts > 0 {
                        tracing::info!(?stats, "compaction pass");
                    }
                }
                _ = finalize_ticker.tick() => {
                    let partitions = self.finalize_once().await?;
                    if partitions > 0 {
                        tracing::info!(partitions, "finalized cold partitions");
                    }
                }
            }
        }
```

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all PASS, including the new finalization test and every pre-existing test (the sweep is a no-op for partitions with recent L0s or a single run, so races/deletion tests are unaffected).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-compactor --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: cold-partition finalization folds quiet partitions into one run"
```

---

### Task 6: ukield config + bootstrap + example config + full suite

**Files:**
- Modify: `crates/ukield/src/config.rs`
- Modify: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/bootstrap.rs`
- Modify: `ukield.example.toml`
- Test: `crates/ukield/tests/bootstrap_test.rs` (+ config tests inside `config.rs` if any name `min_l0_files` — fix compile-driven)

**Interfaces:**
- Consumes: `CompactorConfig` fields (Task 4), `Placement::SizeTargeted` + `set_placement` (Tasks 1–2).
- Produces: `[compactor]` keys `fanout`, `finalize_after_secs`, `finalize_poll_interval_ms` (**`min_l0_files` removed**); `[[tables]]` key `target_file_mb` (mutually exclusive with `placement`). Docker required for bootstrap tests.

- [ ] **Step 1: Write the failing bootstrap test**

Append to `crates/ukield/tests/bootstrap_test.rs` (mirror the file's existing pattern for building a `TableConfig` and asserting on the created hypertable; adapt fixture/helper names to its actual ones):

```rust
#[tokio::test]
async fn bootstrap_sets_size_targeted_placement_from_target_file_mb() {
    let env = setup().await;
    let mut table = base_table_config("events_sized");
    table.target_file_mb = Some(256);

    bootstrap::apply(&env.catalog, std::slice::from_ref(&table))
        .await
        .unwrap();

    let ht = env.catalog.get_hypertable("events_sized").await.unwrap();
    assert_eq!(ht.placement, ukiel_core::Placement::SizeTargeted(256 * 1024 * 1024));
}

#[tokio::test]
async fn bootstrap_rejects_placement_and_target_file_mb_together() {
    let env = setup().await;
    let mut table = base_table_config("events_conflict");
    table.placement = Some("separated".to_string());
    table.target_file_mb = Some(256);

    let err = bootstrap::apply(&env.catalog, std::slice::from_ref(&table))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukield --test bootstrap_test`
Expected: compile error (`target_file_mb` field doesn't exist; `ukield` also still references `min_l0_files`).

- [ ] **Step 3: Implement**

1. `crates/ukield/src/config.rs` — replace `CompactorSection` and its `Default`:

```rust
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct CompactorSection {
    pub poll_interval_ms: u64,
    /// L0 merge trigger. L0 files are unpruned by key (every query reads
    /// all of them): keep low.
    pub l0_fanout: usize,
    /// Run-count merge trigger for L1 and above. Runs are key-disjoint
    /// (<= 1 file per query each), merges move real bytes: keep higher.
    pub fanout: usize,
    /// Fold a partition into a single run once it has had no L0 arrivals
    /// for this long.
    pub finalize_after_secs: u64,
    /// Cadence of the finalization sweep.
    pub finalize_poll_interval_ms: u64,
}

impl Default for CompactorSection {
    fn default() -> Self {
        Self {
            poll_interval_ms: 5_000,
            // Production shape (unlike the test-compat library defaults):
            // eager L0, lazy L1+ — bounds L0 read amp at 4 while total
            // write amp stays ~depth = log10(runs).
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 3_600,
            finalize_poll_interval_ms: 60_000,
        }
    }
}
```

Add to `TableConfig` (next to the existing `placement: Option<String>` field, `crates/ukield/src/config.rs:161`):

```rust
    /// Size-targeted placement: compaction cuts merge outputs at key
    /// boundaries into files of roughly this many megabytes; keys bigger
    /// than the target get dedicated files. Mutually exclusive with
    /// `placement`.
    pub target_file_mb: Option<u64>,
```

Fix any `config.rs` unit test that constructs/asserts `CompactorSection` (compile errors point at them; replace `min_l0_files: 2` with the three new fields or `..Default::default()`).

2. `crates/ukield/src/run.rs:134` — replace the `CompactorConfig` literal:

```rust
            CompactorConfig {
                poll_interval_ms: cfg.compactor.poll_interval_ms,
                l0_fanout: cfg.compactor.l0_fanout,
                fanout: cfg.compactor.fanout,
                finalize_after_secs: cfg.compactor.finalize_after_secs,
                finalize_poll_interval_ms: cfg.compactor.finalize_poll_interval_ms,
                ..CompactorConfig::default()
            },
```

3. `crates/ukield/src/bootstrap.rs` — replace the two lines

```rust
                if table.placement.as_deref() == Some("separated") {
                    catalog.set_placement(id, Placement::Separated).await?;
                }
```

with:

```rust
                let placement = placement_from(table)?;
                if placement != Placement::Packed {
                    catalog.set_placement(id, placement).await?;
                }
```

and append at the bottom of the file:

```rust
/// Resolves the configured placement. `placement` (legacy strings) and
/// `target_file_mb` are mutually exclusive.
fn placement_from(table: &TableConfig) -> anyhow::Result<Placement> {
    match (table.placement.as_deref(), table.target_file_mb) {
        (Some(_), Some(_)) => anyhow::bail!(
            "table '{}': placement and target_file_mb are mutually exclusive",
            table.name
        ),
        (_, Some(mb)) => Ok(Placement::SizeTargeted(mb as i64 * 1024 * 1024)),
        (Some("separated"), None) => Ok(Placement::Separated),
        (Some("packed"), None) | (None, None) => Ok(Placement::Packed),
        (Some(other), None) => {
            anyhow::bail!("table '{}': unknown placement '{other}'", table.name)
        }
    }
}
```

4. `ukield.example.toml` — replace the `[compactor]` section:

```toml
[compactor]
poll_interval_ms = 5000
l0_fanout = 2            # L0 merge trigger (demo; prod ~4) — L0 files are unpruned by key
fanout = 2               # L1+ run-count trigger (demo; prod ~10) — runs are key-disjoint
finalize_after_secs = 60 # fold quiet partitions into one run (demo; prod ~3600)
```

and under the `[[tables]]` comment block add:

```toml
# Placement at rest: default packed; `placement = "separated"` gives every
# tenant dedicated files; `target_file_mb = 256` packs small tenants into
# ~256 MB files while big tenants separate organically.
```

- [ ] **Step 4: Verify everything**

Run: `cargo test -p ukield -- --test-threads=2`
Expected: new bootstrap tests + all existing ukield tests PASS.

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: whole workspace green (the e2e harness compiles untouched — it never set `min_l0_files`).

Run (Docker, ~minutes): `make e2e`
Expected: all scenarios green — S6 (compaction equivalence) and S7 (key deletion) exercise the new merge path end-to-end; default fanout 2 preserves their expectations.

- [ ] **Step 5: Lint and commit**

```bash
git add crates/ukield ukield.example.toml
git commit -m "feat: compactor fanout/finalization config and target_file_mb placement"
```

---

### Task 7: Docs — concept note, README, design/roadmap status flips

**Files:**
- Create: `docs/notes/2026-07-06-lsm-hierarchy.md`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` (status wording only — the design content was written at planning time)
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Full verification first**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green.

- [ ] **Step 2: Write the concept note**

Create `docs/notes/2026-07-06-lsm-hierarchy.md`:

```markdown
# LSM level hierarchy, runs, and size-targeted placement

Status: implemented (plan 17, `docs/superpowers/plans/2026-07-06-ukiel-lsm-hierarchy.md`).

## The model

- **Run** — the key-disjoint output set of one merge commit. Derived, not
  stored: distinct `created_by_commit` among a partition's live parts at a
  level. Each L0 file is its own run.
- **Level ladder** — when a partition holds enough runs at level *n*, they
  merge into one run at level *n+1*. The trigger is **two-tier**: a low
  `l0_fanout` for level 0 and a higher `fanout` for L1+, because the levels
  are asymmetric — L0 files span ~all tenants (no key pruning: read amp is
  linear in their count, and they're tiny, so merging is cheap), while L1+
  runs are key-disjoint (a query touches <= 1 file per run) and their
  merges move real bytes. Same shape as RocksDB's L0 trigger vs level
  multiplier, and as lazy leveling (tier the young levels, level the last —
  finalization is the "level the last one" half). `level` is an `i16`;
  depth is emergent (~log_fanout(flushes per partition)), not hardcoded.
  One merge per partition per pass; the ladder cascades across passes.
- **Finalization** — a partition with no L0 arrivals for
  `finalize_after_secs` and >1 run folds into a single run one level above
  its max. Terminal invariant: **a cold partition is one run**, i.e. all its
  files are pairwise key-disjoint.
- **Placement = one knob** (`hypertables.target_file_bytes`): NULL = packed
  (one file per merge), 0 = separated (every key its own files), N =
  size-targeted (cut merge output at key boundaries at ~N bytes; a key run
  bigger than N gets a dedicated `min == max` file — never split mid-key).

## Why per-file tenant coverage inverts with depth

L0 files are wide in key-space and thin in time. Under a size target, per-file
key coverage grows with level only until accumulated bytes cross the target;
above the crossover, files cap at N bytes and each covers an ever narrower
key slice. Heavy keys precipitate into dedicated files at exactly the level
where their data justifies it; per-key GDPR deletion of such keys is
metadata-only (`delete_key` already branches on part shape).

## Read amplification

Files within a run are key-disjoint, so a point query for key k touches at
most one file per run (catalog range pruning) — plus every live L0 file,
which no key pruning can skip. Hot partition: <= l0_fanout unpruned L0
files + fanout x log_fanout(N) pruned files. Cold partition: exactly one
file chain (<= 1 file for small keys). The sparse-range false positives that remain are
plan 16's target (roaring key bitmaps).

## Non-goals / deferred

- Whole-partition merges stream through RAM (same as v1 packed merges);
  streaming merge is deferred.
- Split points are recomputed per merge (no remembered boundaries); stable
  cut keys would tighten pre-finalization overlap and are deferred.
- Retention-aware compaction and L2+ scheduling niceties ride the same
  machinery later.
```

- [ ] **Step 3: Flip design-doc plan-17 markers and update the README**

In `docs/superpowers/specs/2026-07-05-ukiel-design.md`: the placement/compaction sections already describe this design tagged "(plan 17…)" — change those tags from "planned/ready to execute" wording to implemented, and point at `docs/notes/2026-07-06-lsm-hierarchy.md`. In **Known gaps & deferred work**, remove the level-ladder/size-targeted entry.

In `README.md`, replace the `crates/ukiel-compactor` bullet's text with:

```markdown
- `crates/ukiel-compactor` — background rewrite workers on the change feed:
  fanout-driven level-ladder compaction (a run = one commit's key-disjoint
  output; runs merge a level up at a two-tier trigger — eager `l0_fanout`
  for the unpruned L0 files, lazier `fanout` for L1+), placement policies
  `packed` / `separated` / size-targeted (`target_file_mb`: small tenants
  share files, heavy tenants separate organically), cold-partition
  finalization into a single key-disjoint run, and atomic key deletion
  (metadata-only for dedicated files, rewrite for packed ones). All swaps
  are optimistic REPLACE commits; losers replan from fresh state.
```

- [ ] **Step 4: Roadmap status**

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, row 17 already exists — set its status column to **Executed**.

- [ ] **Step 5: Commit**

```bash
git add docs/ README.md
git commit -m "docs: lsm hierarchy note, size-targeted placement in readme, roadmap row 17 executed"
```

---

## Self-review notes

- **Spec coverage**: size-targeted splitting (T3/T4), whale isolation (T4 test), runs without schema change (T4), multi-level ladder (T4), finalization + disjointness invariant (T5), config surface (T6), docs (T7). Deletion/GC/query intentionally untouched — verified they key off part shape, not placement.
- **Type consistency**: `Placement::SizeTargeted(i64)` / `from_db(Option<i64>)` / `to_db()` used identically in T1/T2/T6; `plan_chunks(&RecordBatch, &str, &Placement, f64) -> Vec<(i64, i64, RecordBatch)>` matches T4's call; `partition_l0_quiet_since(.., f64)` matches T5's `as f64` call; `merge_parts(&Hypertable, &[Part], i16)` used by both T4 and T5.
- **Behavior preservation**: library defaults `l0_fanout: 2, fanout: 2` ≡ old `min_l0_files: 2` (the production 4/10 shape lives only in the ukield `CompactorSection` defaults); `plan_chunks` Packed/Separated arms are literal transplants of the old `match`; existing test suites are the safety net.
