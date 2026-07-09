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
}

pub async fn setup() -> Env {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
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
async fn competing_compactors_never_lose_data() {
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

    // Two compactors with distinct cursors plan from the same live state.
    let c1 = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            worker_name: "c1".into(),
            ..CompactorConfig::default()
        },
    );
    let c2 = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            worker_name: "c2".into(),
            ..CompactorConfig::default()
        },
    );
    let (r1, r2) = tokio::join!(c1.run_once(), c2.run_once());
    let (s1, s2) = (r1.unwrap(), r2.unwrap());

    // Whatever interleaving happened — one merged, both merged sequentially,
    // or one conflicted — the data is intact and exactly one L1 lineage wins.
    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let total: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total, 4);
    assert!(
        s1.merged_groups + s2.merged_groups >= 1,
        "at least one pass merged: {s1:?} {s2:?}"
    );
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
    assert_eq!(
        whale_part.meta.packing_key_max, 42,
        "whale file is min==max"
    );
    assert_eq!(whale_part.meta.row_count, 100);
    let small_part = parts.iter().find(|p| p.meta.packing_key_min == 1).unwrap();
    assert_eq!(
        (
            small_part.meta.packing_key_min,
            small_part.meta.packing_key_max,
            small_part.meta.row_count
        ),
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
async fn oversized_merges_are_capped_not_attempted() {
    let env = setup().await;
    // Two L0 commits on one partition (>= l0_fanout, so the ladder would merge).
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

    // Cap of 1 byte: every real part exceeds it. finalize_after_secs: 0 makes
    // the fresh partition immediately quiet so the finalize path is reached.
    let capped = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            max_merge_input_bytes: 1,
            finalize_after_secs: 0,
            ..CompactorConfig::default()
        },
    );

    // Ladder merge is capped: no merge, both L0 parts stay live.
    let stats = capped.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 0, "capped merge must not run");
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.meta.level == 0));

    // Finalization is capped too: partition stays multi-run.
    assert_eq!(capped.finalize_once().await.unwrap(), 0);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);

    // max_merge_input_bytes: 0 (unlimited) disables the cap. A distinct
    // worker_name gives it a fresh feed cursor so it re-plans the same L0s.
    let unlimited = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig {
            worker_name: "compactor-unlimited".to_string(),
            max_merge_input_bytes: 0,
            ..CompactorConfig::default()
        },
    );
    let stats = unlimited.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "uncapped merge must run");
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert!(
        parts.iter().any(|p| p.meta.level == 1),
        "merged L1 part present"
    );
}
