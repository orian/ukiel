//! Bluesky/JSONBench write-path fixture (plan 30, Task 4/5).
//!
//! Streams real Jetstream ndjson events (`.json.gz`) into Kafka, mapped to the
//! ukiel `bsky` schema, then (Task 5) drives them through the full pipeline —
//! ingest → backpressure → ladder → finalization — the first time plans 17/18
//! run at the volume they were designed for.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, bail};
use flate2::read::MultiGzDecoder;
use rdkafka::producer::FutureRecord;
use rdkafka::producer::future_producer::DeliveryFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ukiel_core::HypertableId;
use ukiel_e2e::Stack;

use crate::hits::TenantCount;

fn dataset_dir() -> PathBuf {
    PathBuf::from("bench/datasets/bluesky")
}

/// The ukiel `bsky` table schema (JSON, `TableColumns` format).
#[allow(dead_code)] // used by `bluesky run` (Task 5)
pub fn bsky_schema_json() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
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
    use std::time::{Duration, Instant};
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
