//! Bluesky/JSONBench write-path fixture (plan 30, Task 4/5).
//!
//! Streams real Jetstream ndjson events (`.json.gz`) into Kafka, mapped to the
//! ukiel `bsky` schema, then (Task 5) drives them through the full pipeline —
//! ingest → backpressure → ladder → finalization — the first time plans 17/18
//! run at the volume they were designed for.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::BufRead;
use std::path::PathBuf;

use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use flate2::read::MultiGzDecoder;
use rdkafka::producer::FutureRecord;
use rdkafka::producer::future_producer::DeliveryFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::{HypertableId, NamespaceId};
use ukiel_e2e::Stack;
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;

use crate::hits::TenantCount;

fn dataset_dir() -> PathBuf {
    PathBuf::from("bench/datasets/bluesky")
}

/// The ukiel `bsky` table schema (JSON, `TableColumns` format).
///
/// `did` (plan 32) is the Bluesky account identifier stored **verbatim**, next
/// to the `tenant_id` it hashes to. JSONBench's q2/q4/q5 group by it, and
/// grouping by the fnv1a hash instead would be collision-equivalent but
/// unreadable — the suite should return actual dids. Fixture-schema addition:
/// a `bsky` fixture produced before this exists cannot serve the official suite;
/// re-produce it (`bench bluesky run`).
pub fn bsky_schema_json() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "did", "type": "utf8"},
        {"name": "kind", "type": "utf8"},
        {"name": "operation", "type": "utf8"},
        {"name": "collection", "type": "utf8"},
        {"name": "payload", "type": "utf8"}
    ]})
}

/// FNV-1a 64-bit hash, sign bit masked off — the design-blessed embedder-side
/// hash turning a string key (`did`) into a positive `int64` packing key.
fn fnv1a64(s: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

/// Maps one Jetstream ndjson line to a flat `bsky` row for the ingest route,
/// or `None` for lines that should flow as poison (unparseable, or missing the
/// `did`/`time_us` a row is keyed and timestamped by). Pure.
fn map_bluesky_event(line: &str) -> Option<Value> {
    let v: Value = serde_json::from_str(line).ok()?;
    let did = v.get("did")?.as_str()?;
    let time_us = v.get("time_us")?.as_i64()?;
    let kind = v.get("kind").and_then(Value::as_str).unwrap_or("");
    let commit = v.get("commit");
    let field = |k: &str| {
        commit
            .and_then(|c| c.get(k))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    // `record` (the post/like/etc. body) is present only on commit creates.
    let payload = commit
        .and_then(|c| c.get("record"))
        .map(|r| r.to_string())
        .unwrap_or_default();
    Some(json!({
        "tenant_id": fnv1a64(did),
        "ts": time_us / 1000,
        "did": did,
        "kind": kind,
        "operation": field("operation"),
        "collection": field("collection"),
        "payload": payload,
    }))
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BskyCensus {
    pub top100: Vec<TenantCount>,
    pub by_kind: BTreeMap<String, i64>,
    pub total_mapped: i64,
    pub poison: i64,
}

#[derive(Debug, Default)]
pub struct ProduceOutcome {
    pub lines: i64,
    pub mapped: i64,
    pub poison: i64,
    pub census: BskyCensus,
}

/// The `file_*.json.gz` files present under the dataset dir, sorted, capped.
fn present_files(limit: usize) -> anyhow::Result<Vec<PathBuf>> {
    let dir = dataset_dir();
    let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "gz"))
            .collect(),
        Err(_) => bail!(
            "no bluesky files under {} — run `make bench-fetch-bluesky`",
            dir.display()
        ),
    };
    files.sort();
    files.truncate(limit);
    if files.is_empty() {
        bail!("no bluesky files under {}", dir.display());
    }
    Ok(files)
}

/// Streams `files` gz files, maps each line, and produces to `topic` with a
/// bounded in-flight window. When `wave` is set, after every `wave_files`
/// files it waits for ingest to catch up (`ingest_offset_sum` within one wave
/// of produced) — bounding Kafka disk for the big tiers. Accumulates the census.
pub async fn stream_and_produce(
    stack: &Stack,
    topic: &str,
    files: usize,
    wave: Option<(HypertableId, usize)>,
) -> anyhow::Result<ProduceOutcome> {
    let paths = present_files(files)?;
    stack.ensure_topic(topic).await;

    let mut counts: HashMap<i64, i64> = HashMap::new();
    let mut out = ProduceOutcome::default();
    let mut inflight: VecDeque<DeliveryFuture> = VecDeque::new();
    let mut produced_since_wave = 0usize;

    for (fi, path) in paths.iter().enumerate() {
        let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = std::io::BufReader::new(MultiGzDecoder::new(file));
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            out.lines += 1;
            let payload = match map_bluesky_event(&line) {
                Some(v) => {
                    out.mapped += 1;
                    if let Some(tid) = v.get("tenant_id").and_then(Value::as_i64) {
                        *counts.entry(tid).or_default() += 1;
                    }
                    if let Some(k) = v.get("kind").and_then(Value::as_str) {
                        *out.census.by_kind.entry(k.to_string()).or_default() += 1;
                    }
                    v.to_string()
                }
                None => {
                    // Flows as poison — ingest skips it, counted here.
                    out.poison += 1;
                    line
                }
            };
            // Non-blocking enqueue; on a full queue, drain one and retry with
            // the same (still-owned) payload.
            loop {
                let record = FutureRecord::<(), _>::to(topic).payload(&payload);
                match stack.producer.send_result(record) {
                    Ok(fut) => {
                        inflight.push_back(fut);
                        break;
                    }
                    Err(_) => match inflight.pop_front() {
                        Some(f) => {
                            let _ = f.await;
                        }
                        None => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                    },
                }
            }
            if inflight.len() >= 50_000 {
                while inflight.len() > 25_000 {
                    if let Some(f) = inflight.pop_front() {
                        let _ = f.await;
                    }
                }
            }
        }
        produced_since_wave += 1;
        if let Some((ht, wave_files)) = wave
            && produced_since_wave >= wave_files
        {
            drain_inflight(&mut inflight).await;
            wait_for_ingest(stack, ht, topic, out.mapped).await;
            produced_since_wave = 0;
        }
        println!(
            "produced file {}/{}: {}",
            fi + 1,
            paths.len(),
            path.display()
        );
    }
    drain_inflight(&mut inflight).await;
    out.census.total_mapped = out.mapped;
    out.census.poison = out.poison;

    let mut sorted: Vec<TenantCount> = counts
        .into_iter()
        .map(|(id, rows)| TenantCount { id, rows })
        .collect();
    sorted.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.id.cmp(&b.id)));
    out.census.top100 = sorted.into_iter().take(100).collect();
    Ok(out)
}

async fn drain_inflight(inflight: &mut VecDeque<DeliveryFuture>) {
    while let Some(f) = inflight.pop_front() {
        let _ = f.await;
    }
}

/// Waits until ingest has durably accounted for at least `target` messages
/// (its offset sum), so Kafka isn't holding more than ~one wave uncompressed.
async fn wait_for_ingest(stack: &Stack, ht: HypertableId, topic: &str, target: i64) {
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        let sum = stack.ingest_offset_sum(ht, topic).await;
        if sum >= target || Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// `bench bluesky produce --files N [--topic T]`: standalone producer (no
/// in-process ingest, so `--wave-files` does not apply here — use `run`).
pub async fn produce(files: usize, topic: &str) -> anyhow::Result<()> {
    let stack = Stack::start().await;
    let outcome = stream_and_produce(&stack, topic, files, None).await?;
    let out = crate::report::write_json("bsky-tenants.json", &outcome.census)?;
    println!(
        "produced {} lines ({} mapped, {} poison) to '{topic}'; census → {}",
        outcome.lines,
        outcome.mapped,
        outcome.poison,
        out.display()
    );
    Ok(())
}

#[derive(Debug, Default, Serialize)]
struct LevelStat {
    parts: usize,
    bytes: i64,
}

#[derive(Debug, Serialize)]
struct TenantQuery {
    id: i64,
    census_rows: i64,
    observed_rows: i64,
    scenarios: Vec<(String, f64)>,
}

#[derive(Debug, Serialize)]
struct BskyRunReport {
    label: String,
    lines: i64,
    mapped: i64,
    poison: i64,
    produce_secs: f64,
    produce_rows_per_s: f64,
    ingest_catchup_secs: f64,
    stored_rows: i64,
    offset_sum: i64,
    compaction_secs: f64,
    finalization_secs: f64,
    parts_by_level: BTreeMap<i16, LevelStat>,
    backpressure_deferrals: u64,
    tenant_queries: Vec<TenantQuery>,
    invariant_violations: Vec<String>,
    metrics_dump: String,
}

/// Total committed rows and per-level part stats from the catalog — the
/// storage-layer view of what ingest + compaction produced.
async fn storage_snapshot(stack: &Stack, ht: HypertableId) -> (i64, BTreeMap<i16, LevelStat>) {
    let parts = stack
        .catalog
        .live_parts(ht, None)
        .await
        .expect("live_parts");
    let mut rows = 0i64;
    let mut by_level: BTreeMap<i16, LevelStat> = BTreeMap::new();
    for p in &parts {
        rows += p.meta.row_count;
        let e = by_level.entry(p.meta.level).or_default();
        e.parts += 1;
        e.bytes += p.meta.size_bytes;
    }
    (rows, by_level)
}

/// Parses a `metric_name{...} value` counter total out of a Prometheus dump
/// (summing all label sets).
fn counter_total(dump: &str, metric: &str) -> u64 {
    dump.lines()
        .filter(|l| l.starts_with(metric) && !l.starts_with('#'))
        .filter_map(|l| l.rsplit(' ').next())
        .filter_map(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
        .sum()
}

/// `bench bluesky run --files N [--wave-files W] [--flush-ms MS] [--label L]`:
/// the full write path in one command — produce → ingest → ladder →
/// finalization — with exactly-once asserted at volume.
pub async fn run(
    files: usize,
    wave_files: Option<usize>,
    flush_ms: u64,
    label: &str,
) -> anyhow::Result<()> {
    // Recorder first, so in-process workers' counters are captured. Installed
    // once per process; `bench` runs a single command, so this is safe.
    let prom = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("install prometheus recorder")?;

    let stack = Stack::start().await;
    let topic = format!("bsky_{}", stack.nonce);
    let ht = stack
        .catalog
        .create_hypertable(
            "bsky",
            &bsky_schema_json(),
            &json!({ "columns": ["day"] }),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .context("create bsky hypertable")?;
    stack.ensure_topic(&topic).await;

    // In-process ingest (production guardrail defaults; wide event-time bound
    // for the historical dataset) and compactor loop (production 4/10 fanouts).
    let ingest_cfg = IngestConfig {
        brokers: stack.brokers.clone(),
        group_id: format!("bench-{}", stack.nonce),
        flush_interval_ms: flush_ms,
        max_buffer_rows: 100_000,
        l0_slowdown_parts: ukiel_ingest::config::defaults::L0_SLOWDOWN_PARTS,
        l0_stop_parts: ukiel_ingest::config::defaults::L0_STOP_PARTS,
        warn_partitions_per_flush: ukiel_ingest::config::defaults::WARN_PARTITIONS_PER_FLUSH,
        max_event_age_days: 30_000, // historical dataset
        max_event_future_secs: 3600,
        tables: vec![TableRoute {
            topic: topic.clone(),
            hypertable: "bsky".to_string(),
            ts_column: "ts".to_string(),
        }],
    };
    let ingest_worker = IngestWorker::new(stack.catalog.clone(), stack.store.clone(), ingest_cfg);
    let ingest_token = CancellationToken::new();
    let ingest_handle = {
        let token = ingest_token.clone();
        tokio::spawn(async move { ingest_worker.run(token).await })
    };
    let compactor = Compactor::new(
        stack.catalog.clone(),
        stack.store.clone(),
        CompactorConfig {
            worker_name: "bench-loop".to_string(),
            l0_fanout: 4,
            fanout: 10,
            poll_interval_ms: 1000,
            ..CompactorConfig::default()
        },
    );
    let compactor_token = CancellationToken::new();
    let compactor_handle = {
        let token = compactor_token.clone();
        tokio::spawn(async move { compactor.run(token).await })
    };

    // Produce (rate-aware if waves configured).
    let wave = wave_files.map(|w| (ht, w));
    let produce_start = Instant::now();
    let outcome = stream_and_produce(&stack, &topic, files, wave).await?;
    let produce_secs = produce_start.elapsed().as_secs_f64();

    // Wait until ingest has consumed every produced message (offsets = lines).
    let catchup_start = Instant::now();
    let offset_sum =
        wait_offset(&stack, ht, &topic, outcome.lines, Duration::from_secs(1800)).await;
    let ingest_catchup_secs = catchup_start.elapsed().as_secs_f64();
    ingest_token.cancel();
    let _ = ingest_handle.await; // clean shutdown → final flush
    compactor_token.cancel();
    let _ = compactor_handle.await;

    // Drain compaction + finalization to quiescence (finalize immediately:
    // ingest stopped, so every partition is quiet).
    let drain = Compactor::new(
        stack.catalog.clone(),
        stack.store.clone(),
        CompactorConfig {
            worker_name: "bench-drain".to_string(),
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            ..CompactorConfig::default()
        },
    );
    let comp_start = Instant::now();
    while drain.run_once().await?.merged_groups > 0 {}
    let compaction_secs = comp_start.elapsed().as_secs_f64();
    let fin_start = Instant::now();
    while drain.finalize_once().await? > 0 {}
    let finalization_secs = fin_start.elapsed().as_secs_f64();

    let (stored_rows, parts_by_level) = storage_snapshot(&stack, ht).await;
    let dump = prom.render();
    let mut violations = Vec::new();
    if stored_rows != outcome.mapped {
        violations.push(format!(
            "stored rows {stored_rows} != mapped events {} (exactly-once at storage)",
            outcome.mapped
        ));
    }
    if offset_sum != outcome.lines {
        violations.push(format!(
            "offset sum {offset_sum} != produced lines {} (ingest did not consume everything)",
            outcome.lines
        ));
    }

    // Per-tenant queryable suite over the top census tenants.
    let mut tenant_queries = Vec::new();
    for tenant in outcome.census.top100.iter().take(3) {
        let ns = NamespaceId(tenant.id);
        let _ = stack.catalog.create_logical_table(ns, "bsky", ht).await; // idempotent-ish
        let observed = query_count(&stack, ns).await;
        if observed != tenant.rows {
            violations.push(format!(
                "tenant {} queryable count {observed} != census {}",
                tenant.id, tenant.rows
            ));
        }
        let range = stack
            .query(ns, "SELECT min(ts) AS lo, max(ts) AS hi FROM bsky")
            .await
            .unwrap_or_else(|_| json!([]));
        let lo = range
            .get(0)
            .and_then(|r| r.get("lo"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let hi = range
            .get(0)
            .and_then(|r| r.get("hi"))
            .and_then(|v| v.as_i64())
            .unwrap_or(lo);
        let mid = lo + (hi - lo) / 2;
        let scenarios_sql = [
            ("count", "SELECT count(*) AS n FROM bsky".to_string()),
            (
                "by_collection",
                "SELECT collection, count(*) AS c FROM bsky GROUP BY collection ORDER BY c DESC LIMIT 10".to_string(),
            ),
            (
                "ts_window",
                format!("SELECT count(*) AS n FROM bsky WHERE ts BETWEEN {mid} AND {}", mid + 3_600_000),
            ),
        ];
        let mut scenarios = Vec::new();
        for (name, sql) in &scenarios_sql {
            let ms = crate::report::warm_median_ms(5, || async {
                stack
                    .query(ns, sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("{name}: {e}"))
            })
            .await?;
            println!("PERF bsky/{}/{name}={ms:.1}", tenant.id);
            scenarios.push((name.to_string(), ms));
        }
        tenant_queries.push(TenantQuery {
            id: tenant.id,
            census_rows: tenant.rows,
            observed_rows: observed,
            scenarios,
        });
    }

    let report = BskyRunReport {
        label: label.to_string(),
        lines: outcome.lines,
        mapped: outcome.mapped,
        poison: outcome.poison,
        produce_secs,
        produce_rows_per_s: if produce_secs > 0.0 {
            outcome.lines as f64 / produce_secs
        } else {
            0.0
        },
        ingest_catchup_secs,
        stored_rows,
        offset_sum,
        compaction_secs,
        finalization_secs,
        parts_by_level,
        backpressure_deferrals: counter_total(&dump, "ingest_backpressure_deferrals_total"),
        tenant_queries,
        invariant_violations: violations.clone(),
        metrics_dump: dump,
    };
    let out = crate::report::write_json(&format!("bsky-run-{label}.json"), &report)?;
    println!(
        "bsky run: {} lines, {} mapped, stored {}, compaction {:.1}s, finalization {:.1}s → {}",
        report.lines,
        report.mapped,
        report.stored_rows,
        report.compaction_secs,
        report.finalization_secs,
        out.display()
    );
    if !violations.is_empty() {
        bail!(
            "write-path invariants violated (report written):\n  {}",
            violations.join("\n  ")
        );
    }
    Ok(())
}

/// JSONBench is 5 queries. A file that parses to any other count is a mis-edit.
const JSONBENCH_QUERIES: usize = 5;

/// `bench bluesky jsonbench [--iters N] [--label L]`: the official JSONBench
/// suite over whatever `bench bluesky run` last ingested — whole-table, so via
/// the unscoped operator session.
///
/// The `bsky` fixture must carry the `did` column (plan 32): a fixture produced
/// before that schema addition cannot answer q2/q4/q5, and the run says so
/// rather than silently reporting on four columns.
pub async fn jsonbench(iters: usize, label: &str) -> anyhow::Result<()> {
    let stack = Stack::start().await;
    let ht = stack
        .catalog
        .get_hypertable("bsky")
        .await
        .context("bsky not loaded — run `bench bluesky run --files N` first")?;
    if !ht.table_schema.to_string().contains("\"did\"") {
        bail!(
            "the loaded `bsky` fixture predates the `did` column (plan 32) — JSONBench's \
             q2/q4/q5 group by the account. Re-produce it: `make e2e-down && bench bluesky run --files N`"
        );
    }
    let parts = stack.catalog.live_parts(ht.id, None).await?;
    let table_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    println!(
        "bsky: {table_rows} rows in {} live parts (the fixture the suite describes)",
        parts.len()
    );

    let queries = crate::suite::load_queries(
        &PathBuf::from("bench/queries/jsonbench/queries.sql"),
        JSONBENCH_QUERIES,
    )?;
    let report = crate::suite::run_suite("ukiel", label, iters, table_rows, &queries, || {
        crate::suite::cold_ukiel_session(&stack)
    })
    .await?;
    crate::suite::finish(report, &format!("jsonbench-ukiel-{label}.json"))?;
    Ok(())
}

/// Polls the ingest offset sum until it reaches `target` or the deadline.
async fn wait_offset(
    stack: &Stack,
    ht: HypertableId,
    topic: &str,
    target: i64,
    timeout: Duration,
) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let sum = stack.ingest_offset_sum(ht, topic).await;
        if sum >= target || Instant::now() >= deadline {
            return sum;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn query_count(stack: &Stack, ns: NamespaceId) -> i64 {
    stack
        .query(ns, "SELECT count(*) AS n FROM bsky")
        .await
        .ok()
        .and_then(|rows| {
            rows.get(0)
                .and_then(|r| r.get("n"))
                .and_then(|n| n.as_i64())
        })
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_positive() {
        let a = fnv1a64("did:plc:abc123");
        let b = fnv1a64("did:plc:abc123");
        assert_eq!(a, b, "stable");
        assert!(a >= 0, "sign bit masked");
        assert_ne!(fnv1a64("did:plc:abc123"), fnv1a64("did:plc:xyz789"));
    }

    #[test]
    fn maps_commit_create_post() {
        let line = r#"{"did":"did:plc:u1","time_us":1700000000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"hi","$type":"app.bsky.feed.post"}}}"#;
        let v = map_bluesky_event(line).unwrap();
        assert_eq!(v["tenant_id"], json!(fnv1a64("did:plc:u1")));
        assert_eq!(v["ts"], json!(1_700_000_000_000i64)); // us / 1000
        assert_eq!(v["kind"], "commit");
        assert_eq!(v["operation"], "create");
        assert_eq!(v["collection"], "app.bsky.feed.post");
        assert!(v["payload"].as_str().unwrap().contains("\"text\":\"hi\""));
    }

    /// Plan 32: JSONBench q2/q4/q5 group by the account, so the `did` is stored
    /// verbatim *alongside* the packing key it hashes to — not instead of it.
    #[test]
    fn maps_did_verbatim_beside_its_hash() {
        let line = r#"{"did":"did:plc:u1","time_us":1700000000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}"#;
        let v = map_bluesky_event(line).unwrap();
        assert_eq!(v["did"], "did:plc:u1", "the did is stored as itself");
        assert_eq!(
            v["tenant_id"],
            json!(fnv1a64("did:plc:u1")),
            "and still hashes to the same packing key as before"
        );
        // Both the schema and the mapper must carry it, or ingest drops it.
        let fields = bsky_schema_json();
        let names: Vec<&str> = fields["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"did"), "did missing from the bsky schema");
    }

    #[test]
    fn maps_identity_event_without_commit() {
        let line = r#"{"did":"did:plc:u2","time_us":1700000001000000,"kind":"identity"}"#;
        let v = map_bluesky_event(line).unwrap();
        assert_eq!(v["kind"], "identity");
        assert_eq!(v["operation"], "");
        assert_eq!(v["collection"], "");
        assert_eq!(v["payload"], "");
    }

    #[test]
    fn maps_delete_without_record() {
        let line = r#"{"did":"did:plc:u3","time_us":1700000002000000,"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.feed.like"}}"#;
        let v = map_bluesky_event(line).unwrap();
        assert_eq!(v["operation"], "delete");
        assert_eq!(v["payload"], "", "no record on a delete");
    }

    #[test]
    fn garbage_and_missing_fields_are_poison() {
        assert!(map_bluesky_event("not json").is_none());
        assert!(map_bluesky_event(r#"{"kind":"commit"}"#).is_none()); // no did/time_us
        assert!(map_bluesky_event(r#"{"did":"did:plc:x"}"#).is_none()); // no time_us
    }
}
