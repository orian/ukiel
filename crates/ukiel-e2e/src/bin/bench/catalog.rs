//! Catalog scalability bench (plan 40): when does the single PostgreSQL primary
//! stop meeting the query-admission and durable-write SLO?
//!
//! Now that cached-MinIO query *execution* is within 3% of bare DataFusion, the
//! catalog is the largest unmeasured horizontal dependency: every query
//! admission and every durable write crosses one primary.
//!
//! Two rules this module exists to enforce, because breaking either produces a
//! number that looks like capacity and is not:
//!
//! * **A ceiling without a target is not a verdict.** Every report carries the
//!   demand scenario it is judged against ([`Demand`]) — registered tenants,
//!   active fraction, query rate, ingest lanes — and the derived read QPS / write
//!   TPS. "Postgres did 40k QPS" answers no question anyone asked.
//! * **A pool is per *process*, not per fleet.** N ukield processes open N × 16
//!   connections to the one primary. One 64-connection pool is not four
//!   processes with 16 each, and treating them as equivalent is how a pool knob
//!   makes a system worse.

// The demand gate and Verdict are unit-tested here and consumed by the workload
// engine in the next task; they are part of this module's contract, not dead.
#![allow(dead_code)]

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

// ---------------------------------------------------------------------------
// Demand model
// ---------------------------------------------------------------------------

/// The product demand a capacity verdict is judged against.
///
/// Benchmark defaults, **not product promises** — they are recorded in every
/// report precisely so a reader can disagree with them and re-derive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Demand {
    pub scenario: String,
    pub registered_tenants: u64,
    /// Fraction of tenants active in a given minute.
    pub active_fraction: f64,
    /// Queries per active tenant per second.
    pub queries_per_active_tenant_per_s: f64,
    /// Ingest lanes (hypertable × Kafka-partition flush streams).
    pub ingest_lanes: u64,
    pub flush_interval_s: f64,
    /// Background workers' tick rates (compactor, finalization, GC, collector).
    pub background_ticks_per_s: f64,
    /// The SLO this scenario is judged against — stated, never invented silently.
    pub query_admission_p99_ms: f64,
    pub commit_p99_ms: f64,
}

/// What one demand scenario costs the catalog, in operations per second.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DerivedLoad {
    /// `live_parts_pruned` + session metadata per product query.
    pub read_qps: f64,
    /// `commit_with_offsets` per ingest flush.
    pub write_tps: f64,
    pub background_qps: f64,
}

impl Demand {
    pub fn steady() -> Self {
        Self {
            scenario: "steady".to_string(),
            registered_tenants: 1_000_000,
            active_fraction: 0.01,
            queries_per_active_tenant_per_s: 1.0 / 60.0, // 1/min
            ingest_lanes: 256,
            flush_interval_s: 10.0,
            background_ticks_per_s: 5.0,
            query_admission_p99_ms: 50.0,
            commit_p99_ms: 100.0,
        }
    }

    pub fn dashboard_surge() -> Self {
        Self {
            scenario: "dashboard-surge".to_string(),
            active_fraction: 0.10,
            ..Self::steady()
        }
    }

    pub fn backfill() -> Self {
        Self {
            scenario: "backfill".to_string(),
            ingest_lanes: 2_048,
            ..Self::steady()
        }
    }

    pub fn named(name: &str) -> anyhow::Result<Self> {
        match name {
            "steady" => Ok(Self::steady()),
            "dashboard-surge" => Ok(Self::dashboard_surge()),
            "backfill" => Ok(Self::backfill()),
            other => bail!("unknown scenario '{other}' (steady | dashboard-surge | backfill)"),
        }
    }

    pub fn derive(&self) -> DerivedLoad {
        let active = self.registered_tenants as f64 * self.active_fraction;
        DerivedLoad {
            read_qps: active * self.queries_per_active_tenant_per_s,
            write_tps: self.ingest_lanes as f64 / self.flush_interval_s.max(f64::MIN_POSITIVE),
            background_qps: self.background_ticks_per_s,
        }
    }

    /// The provisional capacity gate: p99 within the declared SLO, **zero
    /// timeouts**, and headroom over demand — 2× for steady, 1.25× for a surge.
    ///
    /// The asymmetry is deliberate: a steady-state system that can only just
    /// serve steady state has no room to absorb the surge it will certainly see,
    /// while a surge is by definition the thing you are already absorbing.
    pub fn headroom_required(&self) -> f64 {
        if self.scenario.contains("surge") {
            1.25
        } else {
            2.0
        }
    }
}

/// Does a measured result clear the gate for this demand?
// Consumed by the workload engine (Task 2); defined and unit-tested here with
// the demand model it belongs to, because the gate IS the demand model's point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub passes: bool,
    pub reasons: Vec<String>,
    pub achieved_headroom: f64,
}

/// `measured_qps` is the sustainable rate at which p99 held; `p99_ms` the latency
/// there; `timeouts` any deadline miss at all.
pub fn capacity_verdict(demand: &Demand, measured_qps: f64, p99_ms: f64, timeouts: u64) -> Verdict {
    let load = demand.derive();
    let required = demand.headroom_required();
    let achieved = if load.read_qps > 0.0 {
        measured_qps / load.read_qps
    } else {
        f64::INFINITY
    };

    let mut reasons = Vec::new();
    if timeouts > 0 {
        reasons.push(format!("{timeouts} operations missed their deadline"));
    }
    if p99_ms > demand.query_admission_p99_ms {
        reasons.push(format!(
            "p99 {p99_ms:.1} ms exceeds the declared {:.1} ms admission SLO",
            demand.query_admission_p99_ms
        ));
    }
    if achieved < required {
        reasons.push(format!(
            "headroom {achieved:.2}x below the required {required:.2}x over {:.0} demand QPS",
            load.read_qps
        ));
    }
    Verdict {
        passes: reasons.is_empty(),
        reasons,
        achieved_headroom: achieved,
    }
}

/// `bench catalog demand [--scenario S]`: print and serialize the derived load.
pub async fn demand(scenario: &str) -> anyhow::Result<()> {
    let d = Demand::named(scenario)?;
    let l = d.derive();
    println!(
        "scenario {}: {} tenants x {:.0}% active x {:.4} q/s each\n\
         \x20 -> read {:.0} QPS | write {:.1} TPS ({} lanes / {:.0}s flush) | background {:.0} QPS\n\
         \x20 SLO: query-admission p99 <= {:.0} ms, commit p99 <= {:.0} ms; required headroom {:.2}x",
        d.scenario,
        d.registered_tenants,
        d.active_fraction * 100.0,
        d.queries_per_active_tenant_per_s,
        l.read_qps,
        l.write_tps,
        d.ingest_lanes,
        d.flush_interval_s,
        l.background_qps,
        d.query_admission_p99_ms,
        d.commit_p99_ms,
        d.headroom_required(),
    );
    crate::report::write_json(
        &format!("catalog-demand-{scenario}.json"),
        &serde_json::json!({ "demand": d, "derived": l }),
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixture states
// ---------------------------------------------------------------------------

/// The catalog's shape, which matters as much as its size.
///
/// One parts count is not enough: a million *tombstoned* history rows and a
/// million *live* rows exercise entirely different indexes, and a deployment
/// whose live rows all sit in one hot hypertable behaves nothing like one where
/// they are spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    /// New deployment: mostly live, modest history.
    Fresh,
    /// The intended steady state: large history, small live fraction.
    Mature,
    /// Backfill or compactor lag: large history *and* elevated live L0.
    Backlogged,
    /// Failure envelope only: mostly live, concentrated in one hypertable.
    Pathological,
}

impl State {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "fresh" => Ok(State::Fresh),
            "mature" => Ok(State::Mature),
            "backlogged" => Ok(State::Backlogged),
            "pathological" => Ok(State::Pathological),
            other => bail!("unknown --state '{other}' (fresh|mature|backlogged|pathological)"),
        }
    }

    /// Fraction of this state's parts that are **live** (the rest are tombstoned
    /// history, which still sits in the table and still costs vacuum and index
    /// bloat).
    pub fn live_fraction(self) -> f64 {
        match self {
            State::Fresh => 0.90,
            State::Mature => 0.10,
            State::Backlogged => 0.35,
            State::Pathological => 0.95,
        }
    }

    /// Fraction of live parts at L0 — unpruned by key, so the ones that hurt.
    pub fn l0_fraction(self) -> f64 {
        match self {
            State::Fresh => 0.30,
            State::Mature => 0.05,
            State::Backlogged => 0.60,
            State::Pathological => 0.80,
        }
    }

    /// Fraction of live parts concentrated in the single hot hypertable.
    pub fn hot_fraction(self) -> f64 {
        match self {
            State::Fresh => 0.30,
            State::Mature => 0.30,
            State::Backlogged => 0.50,
            State::Pathological => 0.95,
        }
    }
}

/// What a seed actually produced — recorded in the report so a run is
/// self-describing and a mismatched fixture is loud, not silent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedSummary {
    pub label: String,
    pub state: String,
    pub tables: u32,
    /// Issue 0011: the cardinality the *defining* claim is about. Plan 40 seeded
    /// none of these — it measured a catalog with four hypertables and zero
    /// logical tables, and one million packing-key *values*, which is not the
    /// same thing as one million tenants' metadata.
    pub logical_tables: i64,
    pub namespaces: i64,
    pub total_parts: i64,
    pub live_parts: i64,
    pub tombstoned_parts: i64,
    pub hot_table_live_parts: i64,
    pub live_l0_parts: i64,
    pub dedicated_parts: i64,
    pub parts_with_bitmap: i64,
    pub packing_keys: u64,
    pub day_partitions: u32,
    pub parts_table_bytes: i64,
    pub parts_indexes_bytes: i64,
    /// Parts one tenant's `live_parts_pruned` actually returns, sampled across the
    /// key space. Recorded because a capacity number is meaningless without it:
    /// 2 ms returning 7 parts and 2 ms returning 7,000 are not the same system.
    pub tenant_parts_p50: i64,
    pub tenant_parts_p99: i64,
    /// Issue 0014: parts a tenant's query matches by **key range** (what the
    /// catalog returned before 0012) versus by **key set** (what it returns now).
    /// The ratio is the over-fetch — rows shipped so the provider could discard
    /// them.
    pub range_candidates_p50: i64,
    pub exact_parts_p50: i64,
    /// What the shipped catalog returns: range candidates the bounded key filter
    /// cannot disprove. The gap up to `exact_parts_p50` is its false-positive rate,
    /// in parts; the gap down from `range_candidates_p50` is what it removed.
    pub filter_survivors_p50: i64,
    pub filter_survivors_p99: i64,
    /// Bytes the `packing_keys` column and its GIN index cost. The price of the
    /// fix, recorded beside its benefit, because one without the other is a sales
    /// pitch.
    pub packing_keys_bytes: i64,
    pub packing_keys_index_bytes: i64,
    /// Rows in `parts` that are NOT this fixture's — they share the table, its
    /// indexes and its vacuum budget, so they are recorded, not ignored.
    pub foreign_parts: i64,
}

pub const BENCH_TABLE_PREFIX: &str = "bench_cat_";
/// The one logical table every tenant has. `UNIQUE (namespace_id, name)` means a
/// million tenants can all call it `events`, which is what a real deployment
/// looks like — and it makes `get_logical_table` a two-column index probe, the
/// exact shape the admission path issues.
pub const BENCH_LOGICAL_TABLE: &str = "events";
const DAY_PARTITIONS: u32 = 30;

/// What to build. Independent knobs (issue 0011): `State` still supplies the
/// *default* live/history/hot split, but each can be overridden, because the
/// question "does a million-tenant catalog work" is not answerable by a fixture
/// whose shape is derived entirely from one enum.
#[derive(Debug, Clone)]
pub struct SeedSpec {
    pub label: String,
    pub hypertables: u32,
    pub total_parts: i64,
    pub state: State,
    pub packing_keys: u64,
    pub dedicated_frac: f64,
    /// How many keys a *packed* (non-dedicated) part spans. `None` = derive it
    /// from `target_fanout`, which is what you almost always want.
    ///
    /// This is the single most consequential realism knob at scale, and plan 40's
    /// implicit `packing_keys / 20` only looked harmless because its key space was
    /// large and its part count was not. At a million tenants it means every packed
    /// file holds fifty thousand of them, so one tenant's `live_parts_pruned`
    /// matches ~5% of the hypertable — **measured: 1,198 parts** for the median
    /// tenant, where plan 16 measured the *product's* median tenant planning 7
    /// files. A fixture that fans out two orders of magnitude wider than the
    /// product is not measuring the product; it is measuring itself.
    pub key_band: Option<u64>,
    /// Parts the median tenant should match — the physical constraint the band is
    /// solved for. A hypertable's files tile the key space: with `P` live parts
    /// over `K` keys, a file covering `B` keys is matched by a given key about
    /// `P·B/K` times, so a *fixed* band cannot hold fan-out constant while the
    /// part count moves across three orders of magnitude. Solving for the band
    /// instead is what keeps 10k, 1M and 10M parts comparable — and keeps all
    /// three looking like the product.
    ///
    /// Default 7: plan 16's measured median-tenant file count.
    pub target_fanout: f64,
    /// How many times the average table's live parts the *hot* table gets.
    pub hot_multiplier: u32,
    /// Tenants. Each gets `tables_per_namespace` logical tables. The namespace id
    /// *is* the packing key (the v1 convention the provider encodes), so this is
    /// also how many distinct tenant slices the read path can ask for.
    pub namespaces: i64,
    pub tables_per_namespace: u32,
    /// Overrides for the state-derived split. `None` = use the state's fraction.
    pub live_parts: Option<i64>,
    pub historical_parts: Option<i64>,
    pub hot_live_parts: Option<i64>,
}

impl SeedSpec {
    /// `(live, history, hot_live)` — the override wins, the state is the default.
    ///
    /// The hot table is `hot_multiplier` times the *average* table, not a fixed
    /// fraction of the whole live set. `State::hot_fraction` was written for plan
    /// 40's four hypertables, where 30% of the live parts in one table meant 750;
    /// across two thousand it means 300,000 live parts for 500 tenants — six
    /// hundred parts each — which is not a hot table, it is a state compaction
    /// would never leave, and it made the fixture measure something that does not
    /// exist.
    fn split(&self) -> (i64, i64, i64) {
        let live = self.live_parts.unwrap_or_else(|| {
            (self.total_parts as f64 * self.state.live_fraction()).round() as i64
        });
        let history = self.historical_parts.unwrap_or(self.total_parts - live);
        let average = live / self.hypertables.max(1) as i64;
        // Never more than half the live set: a "hot" table that swallowed every
        // part would leave the others empty, and a fixture whose cold tables hold
        // nothing cannot say anything about the tenants that live on them.
        let hot = self
            .hot_live_parts
            .unwrap_or_else(|| average.saturating_mul(self.hot_multiplier as i64));
        (live, history, hot.clamp(average, (live / 2).max(average)))
    }
}

/// `bench catalog seed …` — bulk-build a catalog state.
///
/// **Set-based SQL, not millions of product API calls.** Seeding 10M parts
/// through `commit_with_offsets` would take hours and would measure the seeder,
/// not the system. Write throughput is measured separately, through the real
/// product path, and the two numbers are never conflated. What the seed *does*
/// preserve is everything a query touches: real commits and foreign keys, the
/// live/tombstoned split, L0/L1+ levels, day partitions, key ranges, and plan-16
/// `column_stats` (Int64 bounds, roaring bitmaps, row-group spans) — because row
/// width, TOAST pressure and JSONB decode are part of what we are measuring.
pub async fn seed(spec: SeedSpec) -> anyhow::Result<()> {
    let SeedSpec {
        ref label,
        hypertables: tables,
        total_parts,
        state,
        packing_keys,
        dedicated_frac,
        namespaces,
        tables_per_namespace,
        ..
    } = spec;
    let cat = ukiel_catalog::PostgresCatalog::connect(&pg_url()).await?;
    cat.migrate().await?;
    let pool = cat.pool_for_tests().clone();

    // A measurement column the *product* deliberately does not have. The product
    // stores only the bounded filter (issue 0014: storing the exact key set is
    // what collapsed writes); the fixture keeps the exact set beside it so it can
    // measure what the filter is worth — range candidates against real membership.
    // A fixture may know things the system under test does not; it may not *use*
    // them to answer the system's questions.
    sqlx::query("ALTER TABLE parts ADD COLUMN IF NOT EXISTS packing_keys BIGINT[]")
        .execute(&pool)
        .await?;

    reset_bench_objects(&pool).await?;

    let (live_total, history_total, hot_live) = spec.split();
    // Each hypertable owns a contiguous slice of the tenant space, and both the
    // tenant→hypertable mapping and the parts' key ranges are derived from this
    // one number — if they disagree, tenants resolve to a table that does not hold
    // their key and every `live_parts_pruned` returns zero.
    let keys_per_table = (packing_keys / tables.max(1) as u64).max(1);
    println!(
        "seeding '{label}': {tables} hypertables, {} logical tables over {namespaces} namespaces, \
         {total_parts} parts ({live_total} live / {history_total} tombstoned, {hot_live} hot), \
         state {state:?}, {packing_keys} keys, {keys_per_table} keys per hypertable",
        namespaces * tables_per_namespace as i64
    );

    // One hypertable per table, plus one commit per (table, role) to hang parts
    // off — parts.created_by_commit/deleted_by_commit are real FKs and the query
    // planner's liveness test is `deleted_by_commit IS NULL`, so both must exist.
    let mut ht_ids = Vec::new();
    for t in 0..tables {
        let name = format!("{BENCH_TABLE_PREFIX}{t}");
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO hypertables (name, table_schema, partition_spec, sort_key, packing_key)
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(&name)
        .bind(serde_json::json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .bind(serde_json::json!({"columns": ["day"]}))
        .bind(vec!["tenant_id".to_string(), "ts".to_string()])
        .bind("tenant_id")
        .fetch_one(&pool)
        .await
        .with_context(|| format!("create hypertable {name}"))?;
        ht_ids.push(id);
    }

    // The tenants' metadata — the thing issue 0011 exists for. One row per
    // (namespace, table), namespace id == packing key (the v1 convention the
    // provider encodes: `slice = namespace.0`), spread round-robin over the
    // hypertables. Bulk-inserted, because a million product-API calls would
    // measure the seeder.
    //
    // This is what plan 40 never had: it created hypertables and parts and *no
    // logical tables at all*, then resolved one hypertable once before the run.
    // A million packing-key values is not a million tenants' metadata, and the
    // index depth, the namespace-local listing and the session build are exactly
    // the costs that a key-space knob cannot reach.
    let seeded_logical = if namespaces > 0 && tables_per_namespace > 0 {
        let mut done = 0i64;
        const CHUNK: i64 = 200_000;
        while done < namespaces {
            let batch = CHUNK.min(namespaces - done);
            // The hypertable ids come in as an *array*, indexed by `ns % n`. The
            // obvious `LATERAL (... ORDER BY id OFFSET ns % n LIMIT 1)` re-scans
            // the hypertable list once per tenant — a thousand rows each, a
            // billion in total — and turns a 20-second seed into an overnight one.
            sqlx::query(
                r#"
                INSERT INTO logical_tables (namespace_id, name, hypertable_id)
                SELECT ns,
                       CASE WHEN t = 0 THEN $4 ELSE $4 || '_' || t END,
                       $5[LEAST((ns - 1) / $6, array_length($5, 1) - 1) + 1]
                FROM generate_series($1 + 1, $1 + $2) AS ns,
                     generate_series(0, $3 - 1) AS t
                "#,
            )
            .bind(done)
            .bind(batch)
            .bind(tables_per_namespace as i64)
            .bind(BENCH_LOGICAL_TABLE)
            .bind(&ht_ids)
            .bind(keys_per_table as i64)
            .execute(&pool)
            .await
            .context("bulk logical-table insert")?;
            done += batch;
        }
        namespaces * tables_per_namespace as i64
    } else {
        0
    };

    // Two commits per table: one that created the live parts, one that
    // tombstoned the history. Enough to keep the FKs and the feed honest without
    // pretending to reconstruct a real commit history.
    let mut add_commit = Vec::new();
    let mut del_commit = Vec::new();
    for &ht in &ht_ids {
        for target in [&mut add_commit, &mut del_commit] {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO commits (hypertable_id, kind, idempotency_key)
                 VALUES ($1, 'add', NULL) RETURNING id",
            )
            .bind(ht)
            .fetch_one(&pool)
            .await?;
            target.push(id);
        }
    }

    // The hot hypertable is table 0: `hot_live` of the live parts land there and
    // the rest spread over the others. A uniform spread would hide exactly the
    // failure mode the state model exists to expose.
    let cold_live = live_total - hot_live;
    let history = history_total;

    // The old seeder gave every hypertable parts spanning the whole key space,
    // which is not what a deployment looks like and, at a million tenants over two
    // thousand hypertables, is not even coherent: a table holding 500 tenants was
    // given parts whose key ranges covered all million, so a tenant's
    // `live_parts_pruned` on its own (cold) hypertable returned **zero parts** —
    // 99.95% of the population measuring an empty catalog. The band is therefore
    // solved *per table*, against that table's own tenant count and part count, so
    // every tenant — hot table or cold — sees a realistic fan-out.
    let started = std::time::Instant::now();
    // Remainders are handed to the first tables rather than dropped. Integer
    // division across 2,000 tables silently lost 17% of the parts at the 10k point.
    let mut cold_left = cold_live;
    let mut hist_left = history;
    for (i, &ht) in ht_ids.iter().enumerate() {
        let cold_tables = (tables as i64 - 1).max(1);
        let live_here = if i == 0 {
            hot_live
        } else if tables > 1 {
            let share = cold_live / cold_tables + i64::from((cold_live % cold_tables) >= i as i64);
            share.min(cold_left)
        } else {
            0
        };
        if i > 0 {
            cold_left -= live_here;
        }
        let hist_here = {
            let share = history / tables as i64 + i64::from((history % tables as i64) > i as i64);
            share.min(hist_left)
        };
        hist_left -= hist_here;

        // How many tenants one part holds, solved so a tenant sees `target_fanout`
        // parts. The identity is `pairs = P·S = T·F`: with `P` live parts in a
        // table of `T` tenants, a part holding `S` of them puts each tenant in
        // `P·S/T` parts. Solve for S.
        //
        // The parts' keys are then *scattered* across the slice, so each part's
        // min/max brackets nearly all `T` tenants while holding only `S` — and a
        // tenant's range candidates are therefore ~`P` where its real parts are
        // `F`. That ratio, `T/S`, is issue 0014.
        let keys_per_part = spec.key_band.unwrap_or_else(|| {
            let p = live_here.max(1) as f64;
            ((spec.target_fanout * keys_per_table as f64) / p)
                .round()
                .clamp(1.0, keys_per_table as f64) as u64
        });
        let key_lo = i as u64 * keys_per_table + 1;
        if live_here > 0 {
            insert_parts(
                &pool,
                ht,
                add_commit[i],
                None,
                live_here,
                state,
                dedicated_frac,
                i,
                KeySpace {
                    lo: key_lo,
                    span: keys_per_table,
                    keys_per_part,
                },
            )
            .await?;
        }
        if hist_here > 0 {
            // Tombstoned history: same table, `deleted_by_commit` set. It never
            // appears in a live query but it is very much still in the index.
            insert_parts(
                &pool,
                ht,
                add_commit[i],
                Some(del_commit[i]),
                hist_here,
                state,
                dedicated_frac,
                i,
                KeySpace {
                    lo: key_lo,
                    span: keys_per_table,
                    keys_per_part,
                },
            )
            .await?;
        }
    }
    // ANALYZE, or every measurement that follows is against stale statistics and
    // the planner's choices are noise.
    sqlx::query("ANALYZE parts, commits, hypertables")
        .execute(&pool)
        .await?;
    let seed_secs = started.elapsed().as_secs_f64();

    let summary = summarize(&pool, label, state, tables, packing_keys, seeded_logical).await?;
    println!(
        "seeded in {seed_secs:.1}s: {} parts ({} live, {} tombstoned), hot-table live {}, \
         live L0 {}, dedicated {}, with-bitmap {}\n  parts table {:.1} MiB + indexes {:.1} MiB",
        summary.total_parts,
        summary.live_parts,
        summary.tombstoned_parts,
        summary.hot_table_live_parts,
        summary.live_l0_parts,
        summary.dedicated_parts,
        summary.parts_with_bitmap,
        summary.parts_table_bytes as f64 / 1048576.0,
        summary.parts_indexes_bytes as f64 / 1048576.0,
    );
    println!(
        "  per-tenant fan-out (through the tenant's own hypertable): p50 {} parts, p99 {} \
         — plan 16 measured the product's median tenant at 7 files",
        summary.tenant_parts_p50, summary.tenant_parts_p99
    );
    println!(
        "  ISSUE 0014 — the median tenant's parts, at each stage of the query:\n\
         \x20   by RANGE   {:>6}   (candidates the index brackets — what the catalog used to ship)\n\
         \x20   by FILTER  {:>6}   (what the catalog returns now — {:.0}x fewer rows)\n\
         \x20   by KEY SET {:>6}   (what the provider plans — the floor)\n\
         \x20   the filter costs {:.1} MiB in the rows; the range index, now carrying it, is {:.1} MiB",
        summary.range_candidates_p50,
        summary.filter_survivors_p50,
        summary.range_candidates_p50 as f64 / summary.filter_survivors_p50.max(1) as f64,
        summary.exact_parts_p50,
        summary.packing_keys_bytes as f64 / 1048576.0,
        summary.packing_keys_index_bytes as f64 / 1048576.0,
    );
    verify_via_product_api(&cat, &summary).await?;
    crate::report::write_json(&format!("catalog-seed-{label}.json"), &summary)?;
    Ok(())
}

/// Prove the bulk-seeded rows are the real thing: read them back through the
/// **product** path (`live_parts_pruned` — the same call every query admission
/// makes) and decode a `column_stats` bitmap with the same reader the provider
/// uses.
///
/// A set-based seeder is fast precisely because it bypasses the product code, so
/// it can silently produce rows the product cannot use — a `column_stats` blob
/// that does not decode, a key range no pruning predicate matches. Then every
/// number downstream measures a fixture instead of a system. This is the check
/// that stops that, and it runs on every seed rather than on request.
async fn verify_via_product_api(
    cat: &ukiel_catalog::PostgresCatalog,
    summary: &SeedSummary,
) -> anyhow::Result<()> {
    let ht = cat
        .get_hypertable(&format!("{BENCH_TABLE_PREFIX}0"))
        .await
        .context("the seeded hot hypertable must be readable through the product API")?;

    // Unscoped: every live part of the hot table, through the real planner query.
    let all = cat.live_parts_pruned(ht.id, None, &[]).await?;
    if all.len() as i64 != summary.hot_table_live_parts {
        bail!(
            "live_parts_pruned returned {} parts but the fixture claims {} live in the hot table              — the seed does not match what the product can see",
            all.len(),
            summary.hot_table_live_parts
        );
    }

    // Scoped: a key that some part's range covers must prune to a subset, not to
    // nothing and not to everything.
    let probe = all[0].meta.packing_key_min;
    let pruned = cat.live_parts_pruned(ht.id, Some(probe), &[]).await?;
    if pruned.is_empty() {
        bail!("key {probe} lies inside a seeded part's range but pruned to zero parts");
    }
    if pruned.len() > all.len() {
        bail!("pruning returned MORE parts than exist — the predicate is not doing what it claims");
    }

    // The stats must decode with the provider's own reader, not merely be present.
    let with_stats = all.iter().filter(|p| p.meta.column_stats.is_some()).count();
    let decodable = all
        .iter()
        .filter_map(|p| {
            p.meta
                .column_stats
                .as_ref()?
                .get(ukiel_core::stats::PACKING_KEYS_STAT)?
                .as_str()
                .map(|e| ukiel_core::stats::bitmap_contains(e, 1).is_some())
        })
        .filter(|ok| *ok)
        .count();
    // The tenant metadata, through the *same two calls* query admission makes.
    // Plan 40 never made them: it resolved one hypertable once, before the run,
    // and then looped on parts. A seed that claims a million tenants but cannot
    // be read one tenant at a time is the fixture this issue was filed about.
    if summary.namespaces > 0 {
        use ukiel_core::NamespaceId;
        // A tenant in the middle of the id space *and* the middle of its
        // hypertable's slice. A part's key range runs upward from its `kmin`, so
        // the first few keys of a slice are covered by fewer parts than the rest —
        // a tenant sampled exactly on that edge would report zero and say nothing
        // about the fixture.
        let keys_per_table = (summary.packing_keys / summary.tables.max(1) as u64).max(1) as i64;
        let ns = NamespaceId(summary.namespaces / 2 + keys_per_table / 2);
        let listed = cat.list_logical_tables(ns).await.with_context(|| {
            format!("namespace {ns} must list its tables — this is the admission path")
        })?;
        if listed.is_empty() {
            bail!(
                "namespace {ns} lists no logical tables, but the fixture claims {} over {} namespaces",
                summary.logical_tables,
                summary.namespaces
            );
        }
        let one = cat
            .get_logical_table(ns, &listed[0].name)
            .await
            .context("get_logical_table on a listed name")?;
        let mapped = cat.get_hypertable_by_id(one.hypertable_id).await?;
        // And the slice the provider would ask for: `slice == namespace id`.
        let tenant_parts = cat.live_parts_pruned(mapped.id, Some(ns.0), &[]).await?;
        println!(
            "  verified tenant path: namespace {ns} → {} logical table(s) → hypertable '{}' → \
             live_parts_pruned(slice={}) = {} parts",
            listed.len(),
            mapped.name,
            ns.0,
            tenant_parts.len()
        );
    }

    println!(
        "  verified via product API: live_parts_pruned {} parts (key {probe} prunes to {}), \
         {with_stats} with stats, {decodable} bitmaps decode",
        all.len(),
        pruned.len()
    );

    // The fixture must prove it is TRUTHFUL, or it can lie again — and the first
    // issue-0014 measurement was wrong precisely because it could. Every one of
    // these would have caught it.
    let bad: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM parts
         WHERE deleted_by_commit IS NULL AND packing_keys IS NOT NULL
           AND ( packing_key_min <> packing_keys[1]
              OR packing_key_max <> packing_keys[array_length(packing_keys, 1)]
              OR EXISTS (SELECT 1 FROM unnest(packing_keys) k
                         WHERE k < packing_key_min OR k > packing_key_max) )",
    )
    .fetch_one(cat.pool_for_tests())
    .await?;
    if bad > 0 {
        bail!(
            "{bad} parts declare a key range that does not bracket their own key set — \
             the fixture's ranges and keys have come apart, and every pruning number \
             measured against it would be meaningless"
        );
    }

    // The stored bitmap must decode back to the very keys the part declares. This
    // is the one the old fixture failed: it wrote one constant blob, unrelated to
    // anything, so the provider's exact filter was being tested against fiction.
    let sample: Vec<(Vec<i64>, Option<serde_json::Value>, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT packing_keys, column_stats, key_filter FROM parts
         WHERE deleted_by_commit IS NULL AND packing_keys IS NOT NULL
           AND column_stats ? 'packing_keys'
         LIMIT 200",
    )
    .fetch_all(cat.pool_for_tests())
    .await?;
    for (keys, stats, stored) in &sample {
        let encoded = stats
            .as_ref()
            .and_then(|s| s.get(ukiel_core::stats::PACKING_KEYS_STAT))
            .and_then(|v| v.as_str())
            .context("a multi-key part must carry a bitmap")?;
        let decoded = ukiel_core::stats::bitmap_keys(encoded)
            .context("the stored bitmap must decode with the product's own reader")?;
        if &decoded != keys {
            bail!(
                "a part's stored bitmap decodes to {} keys but it declares {} — the fixture's \
                 bitmap is fiction, and anything measured through it (the provider's pruning, \
                 the files finally planned) is fiction too",
                decoded.len(),
                keys.len()
            );
        }
        // And the filter *in the row* — not one rebuilt here, which would only prove
        // the builder agrees with itself — must be the one these keys build, and must
        // never deny a key the part holds. A fixture that stored the wrong blob would
        // make the catalog prune truthfully-held parts and the numbers would look
        // wonderful.
        let filter = ukiel_core::keyfilter::build(keys).context("filter builds")?;
        if stored.as_deref() != Some(filter.as_slice()) {
            bail!(
                "a part's stored key_filter is not the filter its {} keys build — the \
                 fixture is measuring a filter the product would never write",
                keys.len()
            );
        }
        for k in keys {
            if !ukiel_core::keyfilter::maybe_contains(&filter, *k) {
                bail!("the key filter denies key {k}, which the part holds — data would be lost");
            }
        }
    }
    println!(
        "  fixture is truthful: {} sampled parts — ranges bracket their keys, bitmaps decode \
         back to them, and the key filter never denies a key the part holds",
        sample.len()
    );

    // Every hypertable must carry the *mix* that was asked for, not a coin flip.
    //
    // This is what caught `random()` being hoisted out of an uncorrelated LATERAL and
    // evaluated once per batch: the hot hypertable came out 100% single-key parts and
    // every other one 100% packed, which no `--dedicated-frac` can express. The
    // fixture looked fine — 2M parts, the right totals, all the truthfulness checks
    // green — and the one table every read probe lands on had no packed parts in it
    // at all, so the issue it exists to measure could not occur there.
    let extreme: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT h.name, count(*),
                count(*) FILTER (WHERE p.packing_key_min = p.packing_key_max)
         FROM parts p JOIN hypertables h ON h.id = p.hypertable_id
         WHERE p.deleted_by_commit IS NULL AND h.name LIKE 'bench_cat_%'
         GROUP BY h.name HAVING count(*) >= 100",
    )
    .fetch_all(cat.pool_for_tests())
    .await?;
    for (name, live, single) in &extreme {
        let frac = *single as f64 / *live as f64;
        if !(0.02..=0.98).contains(&frac) {
            bail!(
                "{name}: {:.0}% of its {live} live parts are single-key — a hypertable that is \
                 all-packed or all-dedicated cannot exhibit the range/key-set gap this fixture \
                 exists to measure, and no --dedicated-frac asks for that. The per-part draw has \
                 collapsed to a per-batch one.",
                frac * 100.0
            );
        }
    }

    // The check that stops a silently *broken* fixture — as opposed to a
    // legitimately sparse one.
    //
    // A median tenant resolving to zero parts is only a bug when there were enough
    // live parts to cover the hypertables in the first place. With 1,000 live parts
    // spread over 2,000 hypertables, most tenants genuinely have no data — and that
    // is a real deployment state (a million registered tenants who have barely
    // ingested), worth measuring and worth saying out loud. The same result *while*
    // there are 100k live parts to go round means the tenant→hypertable mapping has
    // come adrift from the key slices, and every latency under it would be a lie.
    if summary.namespaces > 0 && summary.tenant_parts_p50 == 0 {
        if summary.live_parts >= 2 * summary.tables as i64 {
            bail!(
                "the median tenant's live_parts_pruned returns ZERO parts, but the fixture has \
                 {} live parts over {} hypertables — the tenant→hypertable mapping has come \
                 adrift from the key slices, and every latency measured here would be a lie",
                summary.live_parts,
                summary.tables
            );
        }
        println!(
            "  NOTE: the median tenant resolves to ZERO parts — {} live parts cannot cover {} \
             hypertables. A real state (a million registered tenants, almost no data): this \
             point measures the metadata path against an almost-empty parts table.",
            summary.live_parts, summary.tables
        );
    }
    Ok(())
}

/// The tenant slice one hypertable owns, and how many of its tenants one part
/// actually holds.
#[derive(Debug, Clone, Copy)]
struct KeySpace {
    /// First packing key belonging to this hypertable.
    lo: u64,
    /// How many keys it owns.
    span: u64,
    /// Distinct tenants whose rows are **in** one packed part.
    ///
    /// This — not a key *band* — is what a real writer produces, and getting it
    /// wrong is what made the first version of this fixture unable to show issue
    /// 0014 at all. A part's keys are *scattered* across the tenants that were
    /// active in the window it covers, so its `min`/`max` bracket almost the whole
    /// slice while it holds only `keys_per_part` of them. That gap — range says
    /// yes, the key set says no — is the entire subject of issue 0014, and a
    /// fixture that invents `min`/`max` independently of the key set (as this one
    /// did) cannot exhibit it.
    keys_per_part: u64,
}

/// Bulk-insert one population with `generate_series` — one statement, not one
/// round-trip per part.
#[allow(clippy::too_many_arguments)]
async fn insert_parts(
    pool: &PgPool,
    ht: i64,
    add_commit: i64,
    del_commit: Option<i64>,
    n: i64,
    state: State,
    dedicated_frac: f64,
    table_idx: usize,
    keys: KeySpace,
) -> anyhow::Result<()> {
    // Precomputed once per population: every part draws one of these, so its
    // bitmap decodes back to exactly the keys it declares.
    let (flat, blobs, filters) = key_set_shapes(
        keys.lo,
        keys.span,
        keys.keys_per_part,
        table_idx as u64 * 7919,
    );
    let n_shapes = blobs.len() as i64;

    // Chunked so one statement never builds a multi-GB result set.
    const CHUNK: i64 = 200_000;
    let mut done = 0i64;
    while done < n {
        let batch = CHUNK.min(n - done);
        // The generated row is deliberately realistic, and the key set is the part
        // that matters most (issue 0014):
        //
        //  * a part's keys are a **scattered subset** of its hypertable's tenants —
        //    the tenants active in the window it covers — and `min`/`max` are
        //    *derived from that set*, exactly as the real writers derive them. So a
        //    part's range brackets nearly the whole slice while it holds only a few
        //    of its tenants, which is precisely the gap the key bitmap exists to
        //    close and the catalog cannot currently see;
        //  * `dedicated_frac` of parts are single-key (min == max), which range
        //    pruning already answers exactly and which therefore carry no key set,
        //    exactly as the writers decide;
        //  * levels: `l0_fraction` at L0;
        //  * `column_stats` carries a roaring-shaped blob of the right *width* —
        //    the JSONB cost is part of what is being measured, so a NULL would make
        //    every read look cheaper than it is. Its contents are not the semantic
        //    key set (roaring cannot be encoded in SQL); `packing_keys` is, and it
        //    is what the catalog prunes on.
        sqlx::query(
            r#"
            INSERT INTO parts (
                hypertable_id, path, partition_values, packing_key_min, packing_key_max,
                row_count, size_bytes, level, column_stats, packing_keys, key_filter,
                created_by_commit, deleted_by_commit
            )
            SELECT
                $1,
                'ht/' || $1 || '/L' || lv.level || '/' || gen_random_uuid() || '.parquet',
                jsonb_build_object('day', to_char(DATE '2026-01-01' + ((g % $9)::int), 'YYYY-MM-DD')),
                k.keys[1],
                k.keys[array_length(k.keys, 1)],
                100000 + (random() * 900000)::bigint,
                10000000 + (random() * 130000000)::bigint,
                lv.level::smallint,
                CASE
                  WHEN k.dedicated THEN
                    jsonb_build_object('tenant_id', jsonb_build_object('min', k.keys[1], 'max', k.keys[1]),
                                       'ts', jsonb_build_object('min', d.ts_min, 'max', d.ts_max))
                  ELSE
                    jsonb_build_object(
                      'tenant_id', jsonb_build_object('min', k.keys[1],
                                                      'max', k.keys[array_length(k.keys, 1)]),
                      'ts', jsonb_build_object('min', d.ts_min, 'max', d.ts_max),
                      -- TRUTHFUL: this blob decodes back to exactly `k.keys`, the
                      -- keys this part declares. It used to be one constant blob
                      -- unrelated to anything, which made every number derived from
                      -- it — including the provider's pruning — unbelievable.
                      'packing_keys', $10[k.shape],
                      'key_row_groups', $11::jsonb)
                END,
                -- The exact set, for the fixture's own measurement only.
                k.keys,
                -- The bounded filter the product stores, in the product's bit layout,
                -- built by the product's own builder (`ukiel_core::keyfilter`) and
                -- carried in as a precomputed shape.
                --
                -- A dedicated part gets NULL, exactly as the writers give it NULL: its
                -- min == max == its one key, so the range predicate is already exact
                -- and a filter would prove what the two bigints beside it prove.
                CASE WHEN k.dedicated THEN NULL ELSE $16[k.shape] END,
                $2,
                $3
            -- `level` and `dedicated` are hashed from `g`, not drawn from random().
            --
            -- They were `LATERAL (SELECT random() < $6)`, which reads as a per-row
            -- coin flip and is not one: the sublink does not mention `g`, so
            -- PostgreSQL is free to evaluate it **once** and reuse the row. It did.
            -- Every part in a batch came out the same, so `--dedicated-frac 0.2` was
            -- a coin flip per *batch* — and it landed heads on the hot hypertable,
            -- making it 100% single-key parts while every other table was 100%
            -- packed. A fixture that silently quantises its own mix to all-or-nothing
            -- is not a fixture.
            --
            -- Hashing `g` is per-row by construction (and reproducible, which random()
            -- never was). The two constants are different primes, so the level and
            -- dedicated draws stay independent of each other.
            FROM generate_series(1, $4) AS g,
                 LATERAL (SELECT (((g * 2246822519 + $5) % 1000) < ($6 * 1000)::int)::int
                          AS level) lv,
                 LATERAL (SELECT ((g * 1000003 + $5) % 1000) < ($7 * 1000)::int
                          AS dedicated) ded,
                 LATERAL (SELECT (g % $14)::int + 1 AS shape) sh,
                 LATERAL (
                     SELECT ded.dedicated, sh.shape,
                            CASE WHEN ded.dedicated THEN
                                -- A single-key part: its own key, and min == max.
                                ARRAY[$13 + ((g * 2654435761 + $5) % $8)::bigint]
                            ELSE
                                -- One of the precomputed shapes, sliced out of the
                                -- flat key pool. Scattered across the slice, which is
                                -- what makes min/max span far more tenants than the
                                -- part holds — the subject of issue 0014.
                                (SELECT array_agg(DISTINCT x ORDER BY x)
                                 FROM unnest($15[((sh.shape - 1) * $12 + 1)
                                                 : (sh.shape * $12)]) AS x)
                            END AS keys
                 ) k,
                 -- The ts bounds must track the part's *own* day, or a key+time
                 -- probe prunes all-or-nothing and the time predicate measures
                 -- nothing.
                 LATERAL (SELECT 1767225600000::bigint + (g % $9) * 86400000 AS ts_min,
                                 1767225600000::bigint + (g % $9) * 86400000 + 86399999 AS ts_max) d
            "#,
        )
        .bind(ht)
        .bind(add_commit)
        .bind(del_commit)
        .bind(batch)
        .bind(done + table_idx as i64 * 7919) // decorrelate keys across tables
        .bind(state.l0_fraction())
        .bind(dedicated_frac)
        .bind(keys.span as i64)
        .bind(DAY_PARTITIONS as i64)
        .bind(&blobs)
        .bind(fake_spans())
        .bind(keys.keys_per_part as i64)
        .bind(keys.lo as i64)
        .bind(n_shapes)
        .bind(&flat)
        .bind(&filters)
        .execute(pool)
        .await
        .context("bulk part insert")?;
        done += batch;
    }
    Ok(())
}

/// A pool of **truthful** key-set shapes for one hypertable.
///
/// The fixture used to insert one constant roaring blob, unrelated to any part's
/// actual keys or its declared min/max. That is not a small liberty: it makes
/// `column_stats.packing_keys` fiction, so nothing downstream of it — the
/// provider's exact filter, the count of files finally planned — can be believed.
/// It is also how the first issue-0014 measurement came to be wrong.
///
/// Generating a roaring blob per part would mean a round trip per part (2M of
/// them). So: a bounded pool of shapes, precomputed in Rust, chosen by `g % N` in
/// the bulk insert. Every part's bitmap then decodes back to exactly the keys the
/// part declares, and its min/max are those keys' bounds.
///
/// Returns `(flat_keys, blobs)`: the keys of all `n_shapes` shapes concatenated
/// (each `keys_per_part` long, so SQL can slice shape `j` out of it), and the
/// base64 roaring blob of each.
fn key_set_shapes(
    lo: u64,
    span: u64,
    keys_per_part: u64,
    salt: u64,
) -> (Vec<i64>, Vec<String>, Vec<Vec<u8>>) {
    use base64::Engine as _;

    // The shapes must **partition the tenant space**, not sample it.
    //
    // A bounded pool of shapes drawn at random covers only `n_shapes *
    // keys_per_part` distinct tenants — with 512 shapes of 35 keys that is 18k of
    // 100k, so four tenants in five appear in *no part at all* and the fixture's
    // median tenant resolves to nothing. (The seed's own guard caught exactly
    // that, which is the entire reason it exists.)
    //
    // So: shuffle the slice's tenants and deal them into blocks of `keys_per_part`.
    // Every tenant lands in exactly one shape, the shapes are disjoint, and their
    // keys are scattered — which is what makes a part's min/max bracket almost the
    // whole slice while it holds only a handful of tenants.
    let n_shapes = (span / keys_per_part.max(1)).max(1);

    // A deterministic shuffle: walk the slice with a stride coprime to it, so the
    // walk is a permutation.
    let stride = coprime_stride(span, salt);
    let order: Vec<u64> = (0..span).map(|i| (salt + i * stride) % span).collect();

    let (mut flat, mut blobs, mut filters) = (Vec::new(), Vec::new(), Vec::new());
    for shape in 0..n_shapes {
        let start = (shape * keys_per_part) as usize;
        let end = ((shape + 1) * keys_per_part).min(span) as usize;
        let mut keys: Vec<u64> = order[start..end].iter().map(|o| lo + o).collect();
        keys.sort_unstable();
        keys.dedup();

        let mut map = roaring::RoaringTreemap::new();
        for k in &keys {
            map.insert(*k);
        }
        let mut bytes = Vec::new();
        map.serialize_into(&mut bytes).expect("serialize treemap");
        blobs.push(base64::engine::general_purpose::STANDARD.encode(bytes));

        // The product's own filter, from the product's own builder. Built here and
        // not in SQL because the plpgsql builder rewrites the whole blob on every
        // `set_byte` — a hundred rewrites per part, two million parts. Production
        // builds one filter per part, once, which is not that shape at all.
        let exact: Vec<i64> = keys.iter().map(|k| *k as i64).collect();
        filters.push(ukiel_core::keyfilter::build(&exact).expect("a non-empty key set"));

        // Padded to a fixed width so SQL can slice shape `j` out by arithmetic.
        // Duplicates in the flat array are harmless — the *set* is what is stored.
        let mut padded = exact.clone();
        while padded.len() < keys_per_part as usize {
            padded.push(*exact.last().expect("at least one key"));
        }
        flat.extend(padded);
    }
    (flat, blobs, filters)
}

/// A stride coprime to `span`, so `i -> (salt + i*stride) % span` walks every
/// tenant exactly once.
fn coprime_stride(span: u64, salt: u64) -> u64 {
    let mut s = (span / 3).max(1) + salt % 97;
    while gcd(s, span) != 1 {
        s += 1;
    }
    s % span.max(1)
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// Row-group key spans, as plan 16 writes them.
fn fake_spans() -> serde_json::Value {
    serde_json::Value::Array(
        (0..8)
            .map(|i| serde_json::json!([i * 100, i * 100 + 99]))
            .collect(),
    )
}

/// Parts one tenant's `live_parts_pruned` returns — sampled through the tenant's
/// **own** hypertable, exactly as the admission path resolves it.
///
/// Measured, never derived. The fan-out is a consequence of the key band, the
/// dedicated fraction, the part count and the tenant→hypertable mapping all
/// interacting, and the only trustworthy way to know it is to ask the question the
/// product asks. Sampling the hot hypertable alone (the first version of this) is
/// how a fixture reports a healthy 7 while 99.95% of its tenants resolve to zero.
async fn tenant_fanout(pool: &PgPool, namespaces: i64) -> anyhow::Result<Fanout> {
    if namespaces <= 0 {
        return Ok(Fanout::default());
    }
    let (mut exact, mut candidates, mut kept) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..200i64 {
        // Spread across the whole tenant space, so hot-table and cold-table
        // tenants appear in proportion to how many there are.
        let ns = (i * 9973 + 17) % namespaces + 1;
        // The three counts issue 0014 asks for, side by side, for the same tenant:
        //
        //   RANGE  — what the catalog used to ship, and what it still fetches from
        //            the index before deciding;
        //   FILTER — what the shipped catalog actually returns: the range candidates
        //            the bounded Bloom filter cannot disprove. The gap to EXACT is
        //            the filter's false-positive rate, in parts;
        //   EXACT  — the parts that truly hold the tenant, per the truthful bitmap.
        //            This is what the provider plans, and the floor to aim at.
        // The FILTER count uses the product's own predicate, generated by the
        // product's own code — not a copy of it. A benchmark that reimplements the
        // thing it measures is measuring its copy.
        let (c, f, e): (i64, i64, i64) = {
            let pred = ukiel_core::keyfilter::sql_predicate("p.key_filter", 2);
            let mut q = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FILTER (WHERE p.packing_key_min <= $1 AND p.packing_key_max >= $1),
                        count(*) FILTER (WHERE p.packing_key_min <= $1 AND p.packing_key_max >= $1
                                           AND {pred}),
                        count(*) FILTER (WHERE (p.packing_keys IS NOT NULL AND p.packing_keys @> ARRAY[$1])
                                            OR (p.packing_keys IS NULL
                                                AND p.packing_key_min <= $1 AND p.packing_key_max >= $1))
                 FROM parts p
                 JOIN logical_tables lt ON lt.hypertable_id = p.hypertable_id
                 WHERE lt.namespace_id = $1 AND p.deleted_by_commit IS NULL"
            )))
            .bind(ns);
            for b in ukiel_core::keyfilter::sql_binds(ns) {
                q = q.bind(b);
            }
            q.fetch_one(pool).await?
        };
        candidates.push(c);
        kept.push(f);
        exact.push(e);
        // The filter may never lose a part the key set holds. If it ever does, every
        // latency below it is a lie told about a query that returns too little.
        if f < e {
            bail!(
                "tenant {ns}: the key filter keeps {f} parts but the exact key set says {e} — \
                 the filter is pruning parts that hold the tenant's rows"
            );
        }
    }
    candidates.sort_unstable();
    kept.sort_unstable();
    exact.sort_unstable();
    let n = exact.len();
    Ok(Fanout {
        exact_p50: exact[n / 2],
        exact_p99: exact[n * 99 / 100],
        range_p50: candidates[n / 2],
        filter_p50: kept[n / 2],
        filter_p99: kept[n * 99 / 100],
    })
}

/// What one tenant's query touches, at each of the three stages issue 0014 measures.
#[derive(Default, Clone, Copy)]
struct Fanout {
    /// Parts whose exact key set holds the tenant — the floor.
    exact_p50: i64,
    exact_p99: i64,
    /// Parts whose declared key *range* brackets the tenant — what the catalog used
    /// to return.
    range_p50: i64,
    /// Parts the bounded key filter cannot disprove — what the catalog returns now.
    filter_p50: i64,
    filter_p99: i64,
}

#[allow(clippy::too_many_arguments)]
async fn summarize(
    pool: &PgPool,
    label: &str,
    state: State,
    tables: u32,
    packing_keys: u64,
    _seeded_logical: i64,
) -> anyhow::Result<SeedSummary> {
    // Scoped to the bench's OWN hypertables. Counting `FROM parts` unfiltered
    // would fold in whatever else lives in this Postgres (e.g. a ClickBench
    // fixture's 1,102 parts) and silently attribute them to the seeded state —
    // a fixture that misreports itself is worse than no fixture.
    let row = sqlx::query(
        r#"
        WITH bench AS (SELECT id FROM hypertables WHERE name LIKE $1),
             hot AS (SELECT min(id) AS id FROM bench)
        SELECT
          count(*) AS total,
          count(*) FILTER (WHERE deleted_by_commit IS NULL) AS live,
          count(*) FILTER (WHERE deleted_by_commit IS NOT NULL) AS tombstoned,
          count(*) FILTER (WHERE deleted_by_commit IS NULL
                           AND hypertable_id = (SELECT id FROM hot)) AS hot_live,
          count(*) FILTER (WHERE deleted_by_commit IS NULL AND level = 0) AS live_l0,
          count(*) FILTER (WHERE packing_key_min = packing_key_max) AS dedicated,
          count(*) FILTER (WHERE column_stats ? 'packing_keys') AS with_bitmap
        FROM parts WHERE hypertable_id IN (SELECT id FROM bench)
        "#,
    )
    .bind(format!("{BENCH_TABLE_PREFIX}%"))
    .fetch_one(pool)
    .await?;

    // Anything else in this database shares the `parts` table, its indexes and
    // its vacuum budget — so it is not neutral, and the report says how much of
    // it there is rather than pretending the fixture is alone.
    let foreign_parts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM parts WHERE hypertable_id NOT IN
         (SELECT id FROM hypertables WHERE name LIKE $1)",
    )
    .bind(format!("{BENCH_TABLE_PREFIX}%"))
    .fetch_one(pool)
    .await?;
    if foreign_parts > 0 {
        println!(
            "  NOTE: {foreign_parts} non-bench parts share this `parts` table (its indexes,              vacuum and size are shared) — for a clean measurement, `docker compose down -v` first"
        );
    }

    let sizes = sqlx::query("SELECT pg_table_size('parts') AS t, pg_indexes_size('parts') AS i")
        .fetch_one(pool)
        .await?;

    // Counted, never assumed: a seed that claims a million tenants and produced
    // nine hundred thousand is exactly the failure this issue was filed about.
    let (logical_tables, namespaces): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT namespace_id) FROM logical_tables
         WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)",
    )
    .bind(format!("{BENCH_TABLE_PREFIX}%"))
    .fetch_one(pool)
    .await?;

    let fanout = tenant_fanout(pool, namespaces).await?;
    // What the fix actually costs: the filter bytes in the rows, and the index that
    // now carries them in its INCLUDE payload. (The rejected GIN design's cost was
    // an entry per key; this one's is a wider index tuple per part.)
    let (packing_keys_bytes, packing_keys_index_bytes): (i64, i64) = sqlx::query_as(
        "SELECT coalesce(sum(pg_column_size(key_filter)), 0)::bigint,
                pg_relation_size('parts_live_idx')::bigint
         FROM parts WHERE deleted_by_commit IS NULL",
    )
    .fetch_one(pool)
    .await?;

    Ok(SeedSummary {
        label: label.to_string(),
        state: format!("{state:?}").to_lowercase(),
        logical_tables,
        namespaces,
        tenant_parts_p50: fanout.exact_p50,
        tenant_parts_p99: fanout.exact_p99,
        range_candidates_p50: fanout.range_p50,
        exact_parts_p50: fanout.exact_p50,
        filter_survivors_p50: fanout.filter_p50,
        filter_survivors_p99: fanout.filter_p99,
        packing_keys_bytes,
        packing_keys_index_bytes,
        tables,
        total_parts: row.get::<i64, _>("total"),
        live_parts: row.get::<i64, _>("live"),
        tombstoned_parts: row.get::<i64, _>("tombstoned"),
        hot_table_live_parts: row.get::<i64, _>("hot_live"),
        live_l0_parts: row.get::<i64, _>("live_l0"),
        dedicated_parts: row.get::<i64, _>("dedicated"),
        parts_with_bitmap: row.get::<i64, _>("with_bitmap"),
        packing_keys,
        day_partitions: DAY_PARTITIONS,
        parts_table_bytes: sizes.get::<i64, _>("t"),
        parts_indexes_bytes: sizes.get::<i64, _>("i"),
        foreign_parts,
    })
}

/// Drops only `bench_cat_*` hypertables and their rows. The bench writes nothing
/// else, and it runs against a disposable compose Postgres — but "only touches
/// its own objects" is a property worth having by construction rather than by
/// convention.
async fn reset_bench_objects(pool: &PgPool) -> anyhow::Result<()> {
    let like = format!("{BENCH_TABLE_PREFIX}%");
    sqlx::query(
        "DELETE FROM parts WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)",
    )
    .bind(&like)
    .execute(pool)
    .await?;
    // Written out rather than looped over a format!()'d table name: sqlx rejects
    // dynamic SQL strings, and it is right to — a table name spliced from a
    // variable is the shape of an injection even when the variable is a literal.
    let _ = sqlx::query("DELETE FROM pending_objects WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)")
        .bind(&like).execute(pool).await;
    let _ = sqlx::query("DELETE FROM ingest_offsets WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)")
        .bind(&like).execute(pool).await;
    let _ = sqlx::query("DELETE FROM worker_cursors WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)")
        .bind(&like).execute(pool).await;
    let _ = sqlx::query("DELETE FROM logical_tables WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)")
        .bind(&like).execute(pool).await;
    sqlx::query(
        "DELETE FROM commits WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name LIKE $1)",
    )
    .bind(&like)
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM hypertables WHERE name LIKE $1")
        .bind(&like)
        .execute(pool)
        .await?;
    // Reclaim the deleted rows' space. Without this, a reseed reports the sum of
    // every fixture ever seeded — the previous state's dead tuples are still
    // there, and `pg_table_size` counts them. Bloat is a real phenomenon worth
    // measuring, but it must be the *state's* bloat, not the bench's.
    sqlx::query("VACUUM FULL parts").execute(pool).await?;
    Ok(())
}

pub fn pg_url() -> String {
    std::env::var("UKIEL_E2E_PG")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/postgres".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The demand arithmetic is the whole basis of every verdict, so it is
    /// pinned rather than trusted: 1M tenants at 1% active issuing 1 query/min
    /// is 10,000 active tenants ÷ 60 s.
    #[test]
    fn demand_derives_the_load_a_verdict_is_judged_against() {
        let d = Demand::steady();
        let l = d.derive();
        assert!(
            (l.read_qps - 10_000.0 / 60.0).abs() < 1e-6,
            "{}",
            l.read_qps
        );
        assert!((l.write_tps - 25.6).abs() < 1e-6, "{}", l.write_tps);

        // A surge is 10x the active tenants, and nothing else.
        let s = Demand::dashboard_surge().derive();
        assert!((s.read_qps - 10.0 * l.read_qps).abs() < 1e-6);
        assert!((s.write_tps - l.write_tps).abs() < 1e-6);

        // Backfill is 8x the write lanes, and nothing else.
        let b = Demand::backfill().derive();
        assert!((b.write_tps - 8.0 * l.write_tps).abs() < 1e-6);
        assert!((b.read_qps - l.read_qps).abs() < 1e-6);
    }

    /// The gate has three independent legs and *any* of them fails the verdict.
    /// A benchmark that reports throughput while quietly dropping deadlines is
    /// the classic way to publish a capacity number that does not exist.
    #[test]
    fn capacity_gate_fails_on_timeouts_slo_or_headroom_independently() {
        let d = Demand::steady();
        let need = d.derive().read_qps * 2.0; // steady requires 2x

        // All three legs clear.
        let v = capacity_verdict(&d, need, 10.0, 0);
        assert!(v.passes, "{v:?}");
        assert!((v.achieved_headroom - 2.0).abs() < 1e-6);

        // A single timeout is disqualifying, however good the throughput.
        let v = capacity_verdict(&d, need * 10.0, 1.0, 1);
        assert!(!v.passes);
        assert!(v.reasons[0].contains("deadline"));

        // p99 over the declared SLO, with ample headroom.
        let v = capacity_verdict(&d, need * 10.0, 999.0, 0);
        assert!(!v.passes);
        assert!(v.reasons.iter().any(|r| r.contains("SLO")));

        // Enough for demand, but no headroom for the surge that will come.
        let v = capacity_verdict(&d, d.derive().read_qps, 10.0, 0);
        assert!(!v.passes);
        assert!(v.reasons.iter().any(|r| r.contains("headroom")));

        // A surge only needs 1.25x — the asymmetry is intentional.
        let s = Demand::dashboard_surge();
        assert!(capacity_verdict(&s, s.derive().read_qps * 1.3, 10.0, 0).passes);
        assert!(!capacity_verdict(&s, s.derive().read_qps * 1.1, 10.0, 0).passes);
    }

    /// The state model is the point of the fixture: a million *tombstoned* rows
    /// and a million *live* rows are not the same catalog, and a mature
    /// deployment is mostly history.
    #[test]
    fn states_differ_in_liveness_l0_and_hot_concentration() {
        assert!(State::Mature.live_fraction() < State::Fresh.live_fraction());
        assert!(State::Backlogged.l0_fraction() > State::Mature.l0_fraction());
        assert!(State::Pathological.hot_fraction() > State::Mature.hot_fraction());
        for s in [
            State::Fresh,
            State::Mature,
            State::Backlogged,
            State::Pathological,
        ] {
            for f in [s.live_fraction(), s.l0_fraction(), s.hot_fraction()] {
                assert!((0.0..=1.0).contains(&f), "{s:?} fraction out of range");
            }
        }
        assert!(State::parse("nope").is_err());
    }

    /// The seeded key sets must be **truthful**: the bitmap a part carries has to
    /// decode back to exactly the keys that part declares, and its min/max have to
    /// bracket them.
    ///
    /// The fixture used to write one constant blob, unrelated to any part's keys.
    /// That is how the first issue-0014 measurement came to be wrong, and nothing
    /// downstream of a fictional bitmap — the provider's pruning, the files finally
    /// planned — could be believed.
    #[test]
    fn seeded_key_sets_are_truthful() {
        let (flat, blobs, filters) = key_set_shapes(1_000, 500, 7, 0);
        // The shapes partition the slice: 500 tenants / 7 per part = 71 shapes.
        assert_eq!(blobs.len(), 71);
        assert_eq!(filters.len(), 71);
        assert_eq!(flat.len(), 71 * 7);

        for (shape, blob) in blobs.iter().enumerate() {
            let mut keys: Vec<i64> = flat[shape * 7..(shape + 1) * 7].to_vec();
            keys.sort_unstable();
            keys.dedup();

            // The bitmap decodes back to the part's own keys — with the reader the
            // provider itself uses.
            assert_eq!(
                ukiel_core::stats::bitmap_keys(blob).as_deref(),
                Some(keys.as_slice()),
                "the stored bitmap must be the part's key set, not a stand-in"
            );
            // Every key is inside the slice the hypertable owns.
            assert!(keys.iter().all(|k| (1_000..1_500).contains(k)), "{keys:?}");
            // And the filter the shape *carries* — the one the fixture will store —
            // never denies a key the part holds. Rebuilding it here would test the
            // builder twice and the fixture not at all.
            let filter = &filters[shape];
            assert_eq!(
                Some(filter.clone()),
                ukiel_core::keyfilter::build(&keys),
                "the shape's stored filter must be the one its keys build"
            );
            for k in &keys {
                assert!(
                    ukiel_core::keyfilter::maybe_contains(filter, *k),
                    "the key filter denied key {k}, which the part holds — data would be lost"
                );
            }
        }
    }

    #[test]
    fn seeded_spans_are_the_real_shape() {
        let spans = fake_spans();
        assert_eq!(spans.as_array().unwrap().len(), 8);
    }
}
