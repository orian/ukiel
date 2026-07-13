#![allow(dead_code)]

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::Array;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_compactor::rewrite;
use ukiel_core::{CommitOp, HypertableId, PartMeta, Placement, arrow_schema_from_json};
use ukiel_ingest::rows_to_parquet;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

pub struct Env {
    pub _pg: ContainerAsync<Postgres>,
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub ht: HypertableId,
    /// So a test can open a *second*, independently killable pool.
    pub url: String,
}

pub async fn setup() -> Env {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    let ht = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Env {
        _pg: pg,
        catalog,
        store,
        ht,
        url,
    }
}

/// Writes one L0 part with the given rows and commits it.
pub async fn add_l0(env: &Env, day: &str, rows: Vec<serde_json::Value>) {
    let schema = arrow_schema_from_json(&events_schema()).unwrap();
    let encoded = rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        &["tenant_id".to_string(), "ts".to_string()],
        rows,
    )
    .unwrap();
    let path = format!("ht/{}/L0/{}.parquet", env.ht, uuid::Uuid::new_v4());
    let size = encoded.bytes.len() as i64;
    env.store
        .put(&Path::from(path.clone()), encoded.bytes.into())
        .await
        .unwrap();
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({ "day": day }),
                    packing_key_min: encoded.key_min,
                    packing_key_max: encoded.key_max,
                    row_count: encoded.row_count,
                    size_bytes: size,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
}

/// All payloads currently readable from live parts (the ground truth).
pub async fn live_payloads(env: &Env) -> HashSet<String> {
    let schema = Arc::new(arrow_schema_from_json(&events_schema()).unwrap());
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let mut out = HashSet::new();
    for part in &parts {
        let batch =
            rewrite::read_parts_to_batch(&env.store, schema.clone(), std::slice::from_ref(part))
                .await
                .unwrap();
        let idx = batch.schema().index_of("payload").unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.insert(arr.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn packed_compaction_merges_within_partition() {
    let env = setup().await;
    // Three L0 files on day1, one on day2.
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 1, "payload": "p1"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 2, "payload": "p2"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 3, "payload": "p3"})],
    )
    .await;
    add_l0(
        &env,
        "d2",
        vec![json!({"tenant_id": 9, "ts": 4, "payload": "p4"})],
    )
    .await;
    let before = live_payloads(&env).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "only day1 has >= 2 L0 files");
    assert_eq!(stats.parts_in, 3);
    assert_eq!(stats.parts_out, 1);
    assert_eq!(stats.conflicts, 0);

    // Data equivalence (invariant I4) and layout expectations.
    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2); // 1 merged L1 (day1) + 1 untouched L0 (day2)
    let l1 = parts.iter().find(|p| p.meta.level == 1).unwrap();
    assert_eq!(l1.meta.row_count, 3);
    assert_eq!((l1.meta.packing_key_min, l1.meta.packing_key_max), (1, 2));
    assert_eq!(l1.meta.partition_values, json!({"day": "d1"}));

    // Idempotence / convergence: a second pass finds nothing to do.
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 0);
}

#[tokio::test]
async fn separated_compaction_gives_each_key_its_own_file() {
    let env = setup().await;
    env.catalog
        .set_placement(env.ht, Placement::Separated)
        .await
        .unwrap();
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 5, "ts": 1, "payload": "x1"}),
            json!({"tenant_id": 6, "ts": 2, "payload": "y1"}),
        ],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 5, "ts": 3, "payload": "x2"})],
    )
    .await;
    let before = live_payloads(&env).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.parts_in, 2);
    assert_eq!(stats.parts_out, 2, "keys 5 and 6 each get a dedicated file");

    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    for p in &parts {
        assert_eq!(p.meta.level, 1);
        assert_eq!(
            p.meta.packing_key_min, p.meta.packing_key_max,
            "separated = one key per file"
        );
    }
}

use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_ingest_and_compaction_preserve_all_rows() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            poll_interval_ms: 50,
            ..CompactorConfig::default()
        },
    );
    let shutdown = CancellationToken::new();
    let worker = {
        let token = shutdown.clone();
        tokio::spawn(async move { compactor.run(token).await })
    };

    // A writer races the compactor: 20 single-row L0 commits.
    for i in 0..20 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": i % 3, "ts": i, "payload": format!("row-{i}")})],
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Let compaction converge, then stop it.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    shutdown.cancel();
    worker.await.unwrap().unwrap();

    // Invariant I5: nothing lost, nothing duplicated — regardless of how many
    // merge rounds won or conflicted.
    let expected: HashSet<String> = (0..20).map(|i| format!("row-{i}")).collect();
    assert_eq!(live_payloads(&env).await, expected);
    let total: i64 = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.meta.row_count)
        .sum();
    assert_eq!(total, 20);

    // Compaction actually happened: fewer live parts than the 20 committed.
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert!(
        parts.len() < 20,
        "expected some merging, got {} parts",
        parts.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn competing_compactors_do_not_duplicate_object_work() {
    // Plan 41's whole point. Before leases, both workers read, merged and
    // uploaded the same partition's Parquet and only *then* discovered at
    // REPLACE that one of them had wasted all of it. Now the loser never opens
    // a file. Data safety is not the assertion any more (REPLACE always gave
    // us that) — efficiency is.
    let env = setup().await;
    for i in 0..4 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    // Each worker gets its own probe over the shared store, so "who touched
    // object storage" is attributable.
    let (s1, p1) = probe_store(env.store.clone(), None);
    let (s2, p2) = probe_store(env.store.clone(), None);
    let c1 = Compactor::new(env.catalog.clone(), s1, worker_config("c1"));
    let c2 = Compactor::new(env.catalog.clone(), s2, worker_config("c2"));

    let (r1, r2) = tokio::join!(c1.run_once(), c2.run_once());
    let (stats1, stats2) = (r1.unwrap(), r2.unwrap());

    assert_eq!(
        stats1.merged_groups + stats2.merged_groups,
        1,
        "one useful merge, not two: {stats1:?} {stats2:?}"
    );
    assert_eq!(
        stats1.conflicts + stats2.conflicts,
        0,
        "the lease, not the REPLACE, resolved the race: {stats1:?} {stats2:?}"
    );

    // Exactly one worker paid for Parquet reads and an upload.
    let touched = [&p1, &p2].map(|p| p.reads() > 0 || p.uploads() > 0);
    assert_eq!(
        touched.iter().filter(|t| **t).count(),
        1,
        "one worker in object storage: p1={:?} p2={:?}",
        (p1.reads(), p1.uploads()),
        (p2.reads(), p2.uploads())
    );

    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.iter().map(|p| p.meta.row_count).sum::<i64>(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn disjoint_partitions_still_compact_in_parallel() {
    // The lease serializes *inside* a partition; across partitions the fleet
    // must still scale. Both workers rendezvous at their pre-commit barrier:
    // if one worker were doing both merges, the other would never arrive and
    // this test would hang — the strongest available proof of parallel work.
    let env = setup().await;
    for day in ["d1", "d2"] {
        for i in 0..2 {
            add_l0(
                &env,
                day,
                vec![json!({"tenant_id": 1, "ts": i, "payload": format!("{day}-{i}")})],
            )
            .await;
        }
    }
    let before = live_payloads(&env).await;

    let rendezvous = Arc::new(tokio::sync::Barrier::new(2));
    let (s1, p1) = probe_store(
        env.store.clone(),
        Some(PreCommit::Rendezvous(rendezvous.clone())),
    );
    let (s2, p2) = probe_store(env.store.clone(), Some(PreCommit::Rendezvous(rendezvous)));
    let c1 = Compactor::new(env.catalog.clone(), s1, worker_config("c1"));
    let c2 = Compactor::new(env.catalog.clone(), s2, worker_config("c2"));

    let (r1, r2) = tokio::join!(c1.run_once(), c2.run_once());
    let (stats1, stats2) = (r1.unwrap(), r2.unwrap());

    assert_eq!(stats1.merged_groups, 1, "{stats1:?}");
    assert_eq!(stats2.merged_groups, 1, "{stats2:?}");
    assert_eq!(stats1.conflicts + stats2.conflicts, 0);
    assert!(p1.uploads() > 0 && p2.uploads() > 0);

    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2, "one merged run per partition: {parts:?}");
}

pub async fn sqlx_update_schema(env: &Env, schema: &serde_json::Value) {
    // Direct DB update: stands in for the future alter_hypertable_add_column.
    sqlx::query("UPDATE hypertables SET table_schema = $1 WHERE id = $2")
        .bind(schema)
        .bind(env.ht.0)
        .execute(env.catalog.pool_for_tests())
        .await
        .unwrap();
}

#[tokio::test]
async fn compaction_backfills_materialized_columns_added_later() {
    let env = setup().await;

    // Write two L0 files with the ORIGINAL schema (no materialized column) —
    // these are "old files" from before the column existed.
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 5, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 6, "payload": "b"})],
    )
    .await;

    // Schema evolves: a materialized column is added (additive, nullable).
    let evolved = json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"},
        {"name": "ts_plus_key", "type": "int64", "materialized": "ts + tenant_id"}
    ]});
    sqlx_update_schema(&env, &evolved).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1);

    // The L1 file physically contains the backfilled materialized column.
    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let schema = std::sync::Arc::new(
        ukiel_core::TableColumns::parse(&ht.table_schema)
            .unwrap()
            .physical_schema(),
    );
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    let batch = rewrite::read_parts_to_batch(&env.store, schema, &parts)
        .await
        .unwrap();
    let idx = batch.schema().index_of("ts_plus_key").unwrap();
    let col: &arrow::array::Int64Array = batch.column(idx).as_any().downcast_ref().unwrap();
    assert_eq!(
        col.values().as_ref(),
        &[6, 7],
        "ts + tenant_id, backfilled and sorted"
    );
}

/// Object store that fails every upload but delegates reads/lists — lets a test
/// exercise the "registered, upload failed, never committed" window.
#[derive(Debug)]
struct FailingPutStore {
    inner: Arc<dyn ObjectStore>,
}

#[async_trait::async_trait]
impl ObjectStore for FailingPutStore {
    async fn put_opts(
        &self,
        _location: &object_store::path::Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::Generic {
            store: "failing",
            source: "put disabled".into(),
        })
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<object_store::path::Path>>,
    ) -> BoxStream<'static, OsResult<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl fmt::Display for FailingPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingPutStore")
    }
}

#[tokio::test]
async fn compaction_registers_intent_before_upload() {
    let env = setup().await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 2, "payload": "b"})],
    )
    .await;

    let failing: Arc<dyn ObjectStore> = Arc::new(FailingPutStore {
        inner: env.store.clone(),
    });
    let compactor = Compactor::new(env.catalog.clone(), failing, CompactorConfig::default());
    let result = compactor.run_once().await;

    assert!(result.is_err(), "upload failure must abort the merge");
    assert!(
        !env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .is_empty(),
        "the L1 path must be registered as intent before it is uploaded"
    );
    // Nothing was committed: the two L0s are still live.
    assert_eq!(env.catalog.live_parts(env.ht, None).await.unwrap().len(), 2);
}

#[tokio::test]
async fn deletion_registers_intent_before_upload() {
    let env = setup().await;
    // A packed file (keys 7 and 8) that a key-7 deletion must rewrite.
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 7, "ts": 1, "payload": "seven"}),
            json!({"tenant_id": 8, "ts": 2, "payload": "eight"}),
        ],
    )
    .await;
    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let failing: Arc<dyn ObjectStore> = Arc::new(FailingPutStore {
        inner: env.store.clone(),
    });

    let result = ukiel_compactor::deletion::delete_key(&env.catalog, &failing, &ht, 7).await;
    assert!(
        result.is_err(),
        "upload failure must abort the deletion rewrite"
    );
    assert!(
        !env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .is_empty(),
        "the rewritten path must be registered as intent before it is uploaded"
    );
}

#[tokio::test]
async fn compaction_builds_a_level_hierarchy() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(), // fanout = 2
    );

    // Two L0 runs -> one L1 run.
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})],
    )
    .await;
    compactor.run_once().await.unwrap();
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.level, 1);

    // Two more L0 runs -> a second L1 run; then two L1 runs -> one L2 run.
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 4, "ts": 40, "payload": "d"})],
    )
    .await;
    compactor.run_once().await.unwrap(); // merges the L0s (level 0 first)
    compactor.run_once().await.unwrap(); // now two L1 runs -> L2

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(
        parts.len(),
        1,
        "cold ladder collapses to one part: {parts:?}"
    );
    assert_eq!(parts[0].meta.level, 2);
    assert!(
        parts[0].meta.path.contains("/L2/"),
        "path: {}",
        parts[0].meta.path
    );
    assert_eq!(
        live_payloads(&env).await,
        ["a", "b", "c", "d"]
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn size_targeted_whale_splits_into_dedicated_slices() {
    // Plan 29 SizeTargeted semantics (streaming): small keys share a file; a key
    // whose run dwarfs the target splits *mid-key* into multiple dedicated
    // (k,k) files whose ts ranges tile without overlap (plan-12 ts pruning +
    // metadata-only retention). Distinct payloads defeat compression so the
    // whale's encoded size genuinely exceeds the target at test scale.
    let env = setup().await;

    const WHALE_ROWS: i64 = 24_000;
    let whale: Vec<_> = (0..WHALE_ROWS)
        .map(|i| json!({"tenant_id": 42, "ts": i, "payload": format!("whale-{i}-{}", i * 7 + 3)}))
        .collect();
    let small: Vec<_> = (1..=4i64)
        .flat_map(|t| {
            (0..2).map(move |i| json!({"tenant_id": t, "ts": i, "payload": format!("s{t}x{i}")}))
        })
        .collect();
    add_l0(&env, "2026-07-01", whale).await;
    add_l0(&env, "2026-07-01", small).await;

    // Target = a fraction of the whale's own bytes: the whale overflows it
    // several times over (multiple slices); the eight small rows never do.
    let live = env.catalog.live_parts(env.ht, None).await.unwrap();
    let whale_bytes: i64 = live
        .iter()
        .filter(|p| p.meta.packing_key_max == 42)
        .map(|p| p.meta.size_bytes)
        .sum();
    let target = whale_bytes / 8;
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

    // Every row survives the streaming merge (whale + the eight small rows).
    assert_eq!(
        parts.iter().map(|p| p.meta.row_count).sum::<i64>(),
        WHALE_ROWS + 8,
        "all rows preserved: {parts:?}"
    );
    // The small keys land somewhere (they share the whale's first slice, since
    // eight rows never reach the target on their own — that file is min<42).
    assert!(
        parts.iter().any(|p| p.meta.packing_key_min < 42),
        "small keys are present: {parts:?}"
    );

    // The whale split into >= 2 dedicated (42,42) files ...
    let mut dedicated_ts: Vec<(i64, i64)> = parts
        .iter()
        .filter(|p| p.meta.packing_key_min == 42 && p.meta.packing_key_max == 42)
        .map(|p| {
            let ts = &p.meta.column_stats.as_ref().unwrap()["ts"];
            (ts["min"].as_i64().unwrap(), ts["max"].as_i64().unwrap())
        })
        .collect();
    assert!(
        dedicated_ts.len() >= 2,
        "whale splits into >= 2 dedicated slices: {parts:?}"
    );
    // ... whose ts ranges tile the whale without overlap ((key,ts)-sorted).
    dedicated_ts.sort();
    for w in dedicated_ts.windows(2) {
        assert!(
            w[0].1 < w[1].0,
            "dedicated slice ts ranges are disjoint: {dedicated_ts:?}"
        );
    }
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
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})],
    )
    .await;
    compactor.run_once().await.unwrap();
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 4, "ts": 40, "payload": "d"})],
    )
    .await;
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
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 5, "ts": 50, "payload": "e"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 6, "ts": 60, "payload": "f"})],
    )
    .await;
    compactor.run_once().await.unwrap(); // -> third L1 run
    compactor.run_once().await.unwrap(); // 3 L1 runs -> L2

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.level, 2);
}

#[tokio::test]
async fn cold_partitions_finalize_into_a_single_run() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            l0_fanout: 100, // the normal ladder never triggers
            fanout: 100,
            finalize_after_secs: 0, // everything is instantly cold
            ..CompactorConfig::default()
        },
    );

    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})],
    )
    .await;
    add_l0(
        &env,
        "2026-07-01",
        vec![json!({"tenant_id": 3, "ts": 30, "payload": "c"})],
    )
    .await;

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
        ["a", "b", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
    );

    // A single run is final: the sweep is idempotent.
    assert_eq!(compactor.finalize_once().await.unwrap(), 0);
}

#[tokio::test]
async fn compaction_outputs_carry_column_stats() {
    let env = setup().await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 10, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 5, "ts": 99, "payload": "b"})],
    )
    .await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    compactor.run_once().await.unwrap();

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let l1 = parts
        .iter()
        .find(|p| p.meta.level == 1)
        .expect("merged L1 part");
    let stats = l1
        .meta
        .column_stats
        .as_ref()
        .expect("stats on compaction output");
    assert_eq!(stats["ts"]["min"], json!(10));
    assert_eq!(stats["ts"]["max"], json!(99));
    assert_eq!(stats["tenant_id"]["min"], json!(2));
    assert_eq!(stats["tenant_id"]["max"], json!(5));
}

#[tokio::test]
async fn merges_stream_without_a_cap() {
    // Plan 29 retired the plan-28 cap: the same fixture the cap would have
    // skipped now merges to completion through the bounded-memory streaming
    // path (issue 0005 closed).
    let env = setup().await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 10, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 20, "payload": "b"})],
    )
    .await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "the merge runs — no cap");

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert!(
        parts.iter().any(|p| p.meta.level == 1),
        "merged L1 part present"
    );
    let rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(rows, 2, "both rows survive the merge");
}

#[tokio::test]
async fn backlog_is_compacted_without_a_new_feed_event() {
    // The crash-after-cursor regression (plan 41): a worker (or a peer sharing
    // the fleet cursor) advanced past the events that created this backlog and
    // then died before merging. No new event will ever arrive for these parts,
    // so a feed-gated pass would leave them unmerged forever.
    let env = setup().await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 2, "payload": "b"})],
    )
    .await;

    let config = CompactorConfig::default();
    let head = env.catalog.feed_head(env.ht).await.unwrap();
    env.catalog
        .set_cursor(&config.worker_name, env.ht, head)
        .await
        .unwrap();

    let compactor = Compactor::new(env.catalog.clone(), env.store.clone(), config);
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(
        stats.merged_groups, 1,
        "live state, not the feed, is the work ledger"
    );
    assert_eq!(env.catalog.live_parts(env.ht, None).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Lease test support (plan 41): who touched object storage, and a hook that can
// freeze a worker in the one moment where a handoff is dangerous — after its
// output is uploaded, before its REPLACE.
// ---------------------------------------------------------------------------

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone)]
pub enum PreCommit {
    /// Park until the test opens the gate; `arrived` says a worker is parked.
    Gate(Arc<Gate>),
    /// Meet N-1 other workers here (proves they are working in parallel).
    Rendezvous(Arc<tokio::sync::Barrier>),
}

#[derive(Debug)]
pub struct Gate {
    arrived: AtomicUsize,
    open: tokio::sync::Semaphore,
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Gate {
            arrived: AtomicUsize::new(0),
            open: tokio::sync::Semaphore::new(0),
        })
    }
    /// Waits until a worker is parked at the gate (i.e. its upload is done and
    /// only the catalog commit is left).
    pub async fn wait_for_arrival(&self) {
        for _ in 0..600 {
            if self.arrived.load(Ordering::SeqCst) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no worker reached the pre-commit gate");
    }
    pub fn open(&self) {
        self.open.add_permits(1);
    }
}

#[derive(Debug, Default)]
pub struct Probe {
    reads: AtomicUsize,
    uploads: AtomicUsize,
}

impl Probe {
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
    pub fn uploads(&self) -> usize {
        self.uploads.load(Ordering::SeqCst)
    }
}

/// Counts one worker's object-store traffic and, optionally, fires `pre_commit`
/// on the `head` of an object *this store wrote* — the last object-store call a
/// merge makes before it commits.
struct ProbeStore {
    inner: Arc<dyn ObjectStore>,
    probe: Arc<Probe>,
    written: Mutex<HashSet<String>>,
    pre_commit: Option<PreCommit>,
}

fn probe_store(
    inner: Arc<dyn ObjectStore>,
    pre_commit: Option<PreCommit>,
) -> (Arc<dyn ObjectStore>, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let store = Arc::new(ProbeStore {
        inner,
        probe: probe.clone(),
        written: Mutex::new(HashSet::new()),
        pre_commit,
    });
    (store, probe)
}

/// A compactor that renews often and expires fast, so a test can drive handoff
/// in seconds rather than a minute.
fn worker_config(name: &str) -> CompactorConfig {
    CompactorConfig {
        worker_name: name.into(),
        lease_ttl_ms: 5_000,
        lease_renew_interval_ms: 200,
        ..CompactorConfig::default()
    }
}

#[async_trait::async_trait]
impl ObjectStore for ProbeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.probe.uploads.fetch_add(1, Ordering::SeqCst);
        self.written
            .lock()
            .unwrap()
            .insert(location.as_ref().to_string());
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.probe.uploads.fetch_add(1, Ordering::SeqCst);
        self.written
            .lock()
            .unwrap()
            .insert(location.as_ref().to_string());
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        // The merge's last object-store call is the `head` of the file it just
        // wrote (`ObjectStoreExt::head` = a head-only `get_opts`), so that is
        // where a test can freeze a worker between "uploaded" and "committed".
        // A head of an input part is an ordinary metadata read, not our hook.
        let ours = options.head && self.written.lock().unwrap().contains(location.as_ref());
        if ours {
            match self.pre_commit.clone() {
                Some(PreCommit::Gate(gate)) => {
                    gate.arrived.fetch_add(1, Ordering::SeqCst);
                    let _permit = gate.open.acquire().await.expect("gate not closed");
                    gate.arrived.fetch_sub(1, Ordering::SeqCst);
                }
                Some(PreCommit::Rendezvous(barrier)) => {
                    barrier.wait().await;
                }
                None => {}
            }
        } else {
            self.probe.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl fmt::Debug for ProbeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProbeStore")
    }
}

impl fmt::Display for ProbeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProbeStore")
    }
}

/// Backdates every lease on a partition: the worker stalled past its TTL.
async fn expire_leases(env: &Env, day: &str) {
    sqlx::query(
        "UPDATE compaction_leases SET expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE hypertable_id = $1 AND partition_values = $2",
    )
    .bind(env.ht.0)
    .bind(json!({ "day": day }))
    .execute(env.catalog.pool_for_tests())
    .await
    .unwrap();
}

async fn lease_generation(env: &Env, day: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT generation FROM compaction_leases
         WHERE hypertable_id = $1 AND partition_values = $2",
    )
    .bind(env.ht.0)
    .bind(json!({ "day": day }))
    .fetch_one(env.catalog.pool_for_tests())
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_owner_cannot_commit_after_handoff() {
    // A is frozen with its output uploaded and only the REPLACE left. Its lease
    // expires and B takes the partition. When A resumes, the fence inside the
    // REPLACE transaction — not a check A performed on itself — is what refuses
    // it. Renewal is turned off (interval > the test) so the *commit* fence is
    // what is under test here, not the renewal loop.
    let env = setup().await;
    for i in 0..2 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    let gate = Gate::new();
    let (store_a, probe_a) = probe_store(env.store.clone(), Some(PreCommit::Gate(gate.clone())));
    let a = Compactor::new(
        env.catalog.clone(),
        store_a,
        CompactorConfig {
            worker_name: "a".into(),
            lease_ttl_ms: 5_000,
            lease_renew_interval_ms: 600_000, // never renews during the test
            ..CompactorConfig::default()
        },
    );
    let a_pass = tokio::spawn(async move { a.run_once().await.unwrap() });

    // A is parked: uploaded, uncommitted.
    gate.wait_for_arrival().await;
    let generation = lease_generation(&env, "d1").await;

    // A dies as far as the catalog can tell, and peer B claims the partition at
    // the next generation.
    expire_leases(&env, "d1").await;
    let b_lease = env
        .catalog
        .try_acquire_compaction_lease(env.ht, &json!({"day": "d1"}), Uuid::new_v4(), TEST_TTL)
        .await
        .unwrap()
        .expect("expired lease is claimable");
    assert_eq!(b_lease.generation, generation + 1);

    // A resumes into a partition it no longer owns. Its REPLACE is refused.
    gate.open();
    let stats_a = a_pass.await.unwrap();
    assert_eq!(
        stats_a.merged_groups, 0,
        "the zombie's REPLACE was fenced out"
    );
    assert_eq!(
        stats_a.conflicts, 0,
        "and it is lease loss, not an optimistic conflict"
    );
    assert!(probe_a.uploads() > 0, "A had already uploaded its output");

    // Nothing of A's landed: its inputs are still live, and the bytes it did
    // upload stay intent-covered for GC (lease loss never clears an intent).
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2, "no input was tombstoned: {parts:?}");
    assert_eq!(
        env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .len(),
        1,
        "A's abandoned upload is a tracked orphan, not a leak"
    );

    // B, holding the partition, does the work A never finished.
    env.catalog
        .release_compaction_lease(&b_lease)
        .await
        .unwrap();
    let (store_b, probe_b) = probe_store(env.store.clone(), None);
    let b = Compactor::new(env.catalog.clone(), store_b, worker_config("b"));
    assert_eq!(b.run_once().await.unwrap().merged_groups, 1);
    assert!(probe_b.uploads() > 0);

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(live_payloads(&env).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_owner_hands_its_backlog_to_a_peer() {
    // A claims the partition and dies without releasing (no graceful shutdown,
    // no new feed event afterwards). The backlog must still converge once the
    // lease expires: a cursor-driven fleet would have lost it forever.
    let env = setup().await;
    for i in 0..2 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    let gate = Gate::new();
    let (store_a, _probe_a) = probe_store(env.store.clone(), Some(PreCommit::Gate(gate.clone())));
    let a = Compactor::new(
        env.catalog.clone(),
        store_a,
        CompactorConfig {
            worker_name: "fleet".into(), // one shared fleet cursor
            lease_ttl_ms: 5_000,
            lease_renew_interval_ms: 600_000,
            ..CompactorConfig::default()
        },
    );
    let a_pass = tokio::spawn(async move { a.run_once().await });
    gate.wait_for_arrival().await;

    // Kill A mid-merge: the task is dropped, the lease is never released.
    a_pass.abort();
    assert!(a_pass.await.unwrap_err().is_cancelled());

    // B shares the cursor name, and A already advanced it past the feed — so B
    // has no unread event to wake on. It still finds the work in live state,
    // and the expired lease lets it claim the partition.
    let (store_b, probe_b) = probe_store(env.store.clone(), None);
    let b = Compactor::new(env.catalog.clone(), store_b, worker_config("fleet"));
    assert_eq!(
        b.run_once().await.unwrap().merged_groups,
        0,
        "A's lease is still live: B leaves the partition alone"
    );
    assert_eq!(probe_b.uploads(), 0, "and does no object work for it");

    expire_leases(&env, "d1").await;
    let stats = b.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "expiry hands the backlog to B");

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(live_payloads(&env).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_merge_survives_on_renewals() {
    // The merge outlives its initial TTL several times over. Renewal is what
    // keeps it legal — and renewal preserves the generation, so the commit it
    // finally makes is still fenced by the claim it started with.
    let env = setup().await;
    for i in 0..2 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    let gate = Gate::new();
    let (store, _probe) = probe_store(env.store.clone(), Some(PreCommit::Gate(gate.clone())));
    let compactor = Compactor::new(
        env.catalog.clone(),
        store,
        CompactorConfig {
            worker_name: "slow".into(),
            lease_ttl_ms: 1_000,
            lease_renew_interval_ms: 100,
            ..CompactorConfig::default()
        },
    );
    let generation_before = {
        let pass = tokio::spawn(async move { compactor.run_once().await.unwrap() });
        gate.wait_for_arrival().await;
        let g = lease_generation(&env, "d1").await;
        // Park for 3x the TTL: without renewal the lease would be long gone.
        tokio::time::sleep(Duration::from_millis(3_000)).await;
        assert_eq!(
            lease_generation(&env, "d1").await,
            g,
            "renewal extends the tenancy without a new generation"
        );
        gate.open();
        let stats = pass.await.unwrap();
        assert_eq!(stats.merged_groups, 1, "the slow merge committed");
        g
    };
    assert_eq!(generation_before, 1);

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(live_payloads(&env).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn renewal_loss_cancels_the_merge_before_replace() {
    // Renewal, not the commit, is the fast path for noticing a handoff: the
    // moment ownership is provably gone, the merge future is dropped. Nothing
    // is tombstoned, and the bytes already uploaded stay intent-covered so GC
    // (not the compactor) is what eventually removes them.
    let env = setup().await;
    for i in 0..2 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    let gate = Gate::new();
    let (store, probe) = probe_store(env.store.clone(), Some(PreCommit::Gate(gate.clone())));
    let compactor = Compactor::new(
        env.catalog.clone(),
        store,
        CompactorConfig {
            worker_name: "loser".into(),
            lease_ttl_ms: 5_000,
            lease_renew_interval_ms: 100,
            ..CompactorConfig::default()
        },
    );
    let pass = tokio::spawn(async move { compactor.run_once().await.unwrap() });
    gate.wait_for_arrival().await;
    assert!(probe.uploads() > 0, "the upload completed before the loss");

    // Someone else takes the partition; the next renewal proves the loss.
    expire_leases(&env, "d1").await;
    let thief = env
        .catalog
        .try_acquire_compaction_lease(env.ht, &json!({"day": "d1"}), Uuid::new_v4(), TEST_TTL)
        .await
        .unwrap()
        .expect("expired lease is claimable");
    assert_eq!(thief.generation, 2);

    // The merge is cancelled while still parked: the gate is never opened.
    let stats = tokio::time::timeout(Duration::from_secs(10), pass)
        .await
        .expect("renewal loss must cancel the merge without opening the gate")
        .unwrap();
    assert_eq!(stats.merged_groups, 0);
    assert_eq!(
        stats.conflicts, 0,
        "lease loss is not an optimistic conflict"
    );

    // Inputs untouched, no output registered, uploaded bytes still tracked.
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2, "no live input was tombstoned: {parts:?}");
    assert_eq!(live_payloads(&env).await, before);
    assert_eq!(
        env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .len(),
        1,
        "the abandoned upload keeps its intent row"
    );
}

const TEST_TTL: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn finalization_and_the_ladder_share_the_partition_lease() {
    // Finalization consumes *every* live part of a partition; a level merge
    // consumes some of them. They must not run at once — so they take the same
    // lease, and the one that arrives second finds the partition held.
    let env = setup().await;
    for i in 0..3 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            l0_fanout: 100, // the ladder never triggers; only finalization does
            fanout: 100,
            finalize_after_secs: 0,
            ..worker_config("f1")
        },
    );

    // A peer holds the partition: finalization must not touch it.
    let peer = env
        .catalog
        .try_acquire_compaction_lease(env.ht, &json!({"day": "d1"}), Uuid::new_v4(), TEST_TTL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compactor.finalize_once().await.unwrap(),
        0,
        "a claimed partition is not finalized under its owner"
    );
    assert_eq!(env.catalog.live_parts(env.ht, None).await.unwrap().len(), 3);

    // Released: finalization claims it and folds the partition into one run.
    env.catalog.release_compaction_lease(&peer).await.unwrap();
    assert_eq!(compactor.finalize_once().await.unwrap(), 1);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(live_payloads(&env).await, before);

    // The lease is handed back after the sweep, not held until expiry.
    assert!(
        env.catalog
            .try_acquire_compaction_lease(env.ht, &json!({"day": "d1"}), Uuid::new_v4(), TEST_TTL)
            .await
            .unwrap()
            .is_some(),
        "the finalizer released the partition"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_outage_during_renewal_stops_the_merge() {
    // Proven loss is not the only way to lose a lease: a catalog we cannot reach
    // leaves ownership *unknown*, and unknown is not permission to continue. The
    // merge must stop there — no unfenced REPLACE, no tombstoned inputs — and
    // the worker must report a real error rather than swallow it as churn.
    let env = setup().await;
    for i in 0..2 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})],
        )
        .await;
    }
    let before = live_payloads(&env).await;

    // The compactor gets its own pool, so closing it is an outage for the
    // worker alone and the test can still observe the catalog afterwards.
    let worker_catalog = PostgresCatalog::connect(&env.url).await.unwrap();
    let gate = Gate::new();
    let (store, probe) = probe_store(env.store.clone(), Some(PreCommit::Gate(gate.clone())));
    let compactor = Compactor::new(
        worker_catalog.clone(),
        store,
        CompactorConfig {
            worker_name: "outage".into(),
            lease_ttl_ms: 5_000,
            lease_renew_interval_ms: 100,
            ..CompactorConfig::default()
        },
    );
    let pass = tokio::spawn(async move { compactor.run_once().await });
    gate.wait_for_arrival().await;
    assert!(probe.uploads() > 0, "uploaded, not yet committed");

    // The catalog goes away underneath the in-flight merge.
    worker_catalog.pool_for_tests().close().await;

    let result = tokio::time::timeout(Duration::from_secs(10), pass)
        .await
        .expect("the merge must be cancelled, not left parked at the gate")
        .unwrap();
    assert!(
        result.is_err(),
        "an unreachable catalog is a worker error, not routine lease churn"
    );

    // Nothing landed: the inputs are live, the data is whole, and the uploaded
    // bytes stay intent-covered for GC.
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2, "no input was tombstoned: {parts:?}");
    assert_eq!(live_payloads(&env).await, before);
    assert_eq!(
        env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .len(),
        1
    );
}
