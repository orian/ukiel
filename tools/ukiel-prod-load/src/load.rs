//! The materialized load: real objects, real catalog metadata, through the product's
//! own write path.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prod_synth_contract::{Manifest, Topology};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, HypertableId, PartMeta};

use crate::{hypertable_name, object_prefix};

pub type LoadError = anyhow::Error;

/// What was loaded, recorded together so a report can always name the exact fixture
/// it measured.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LoadReport {
    pub label: String,
    pub hypertable: String,
    pub hypertable_id: i64,
    pub manifest_digest: String,
    pub topology_digest: String,
    pub seed: u64,
    pub tier: String,
    pub parts: usize,
    pub rows: u64,
    pub bytes: u64,
    pub materialized: bool,
    /// Per representative tenant: what the catalog returns at each stage of pruning.
    pub geometry: Vec<TenantGeometry>,
}

/// The three numbers plan 45 exists to put next to each other.
///
/// `range` is what the catalog's min/max index alone would return. `shipped` is what
/// it actually returns once the issue-0014 key filter has rejected the parts it can
/// prove are absent. `exact` is the truth from the topology. The gap between `range`
/// and `exact` is the over-fetch; the gap between `shipped` and `exact` is what the
/// Bloom filter's false positives cost.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TenantGeometry {
    pub tenant: i64,
    pub class: String,
    pub exact: u64,
    pub range: u64,
    pub shipped: u64,
    pub rows: u64,
}

pub struct Loaded {
    pub report: LoadReport,
    pub hypertable_id: HypertableId,
}

/// Read and verify the artifact **before** connecting to anything.
///
/// A corrupt fixture must fail on the filesystem, not halfway through writing to a
/// catalog. The alternative is a half-loaded hypertable that looks like a real one.
pub fn read_artifact(manifest_path: &Path) -> Result<(Manifest, Topology)> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));

    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest = Manifest::parse(&manifest_path.display().to_string(), &manifest_bytes)?;

    let topology_path = dir.join(&manifest.topology.path);
    let topology_bytes = std::fs::read(&topology_path)
        .with_context(|| format!("reading {}", topology_path.display()))?;
    // The digest the manifest recorded. A topology from another seed is an error here
    // rather than a subtly wrong benchmark later.
    manifest
        .topology
        .verify(&topology_path.display().to_string(), &topology_bytes)?;
    let topology = Topology::parse(&topology_path.display().to_string(), &topology_bytes)?;

    if topology.config != manifest.config {
        bail!("the manifest and topology were generated under different configurations");
    }
    Ok((manifest, topology))
}

/// The label must be fresh. Loading a second fixture over a first is how a benchmark
/// ends up measuring a mixture of two seeds and reporting the mean of them.
pub async fn require_fresh_label(catalog: &PostgresCatalog, label: &str) -> Result<()> {
    let name = hypertable_name(label);
    if catalog.get_hypertable(&name).await.is_ok() {
        bail!(
            "hypertable '{name}' already exists. Labels are not reusable: a fixture loaded over \
             another one produces a catalog that is a mixture of two seeds, and no report drawn \
             from it means anything. Pick a fresh --label, or drop this one deliberately."
        );
    }
    Ok(())
}

/// Create the hypertable and one `events` logical table per queryable tenant.
///
/// The per-tenant part is not decoration. Ukiel's v1 scoping convention is
/// **`slice = packing_key == namespace_id`** (see `ukiel_query::context`), so the
/// namespace a session is opened in *is* the tenant whose rows it can see. A single
/// logical table in namespace 1 would give exactly one queryable tenant — tenant 1 —
/// and the scoped query path, which is the thing plan 45 exists to measure, could not
/// be exercised at all.
///
/// So every tenant a benchmark may ask about gets an `events` table in its own
/// namespace: the five representative classes and the deterministic sample.
pub async fn create_hypertable(
    catalog: &PostgresCatalog,
    m: &Manifest,
    label: &str,
) -> Result<HypertableId> {
    let name = hypertable_name(label);

    // The source's anonymized partition hash, carried as provenance. It is NOT an
    // Ukiel day partition, and nothing downstream may treat it as one: ClickHouse
    // partitions by calendar month, and mapping that onto Ukiel's partitioning would
    // give a real number a false meaning.
    let partition_spec = serde_json::json!({
        "fields": [{"name": "source_partition", "type": "utf8"}],
        "note": "provenance only: the anonymized ClickHouse partition hash. Not an Ukiel day partition."
    });

    let id = catalog
        .create_hypertable(
            &name,
            &m.table.schema,
            &partition_spec,
            &m.table.sort_key,
            &m.table.packing_key,
        )
        .await
        .with_context(|| format!("creating hypertable '{name}'"))?;

    for tenant in queryable_tenants(m) {
        catalog
            .create_logical_table(ukiel_core::NamespaceId(tenant), "events", id)
            .await
            .with_context(|| format!("creating 'events' for tenant {tenant}"))?;
    }

    Ok(id)
}

/// The tenants a benchmark may open a scoped session as: the five representative
/// classes plus the deterministic sample, deduplicated.
pub fn queryable_tenants(m: &Manifest) -> Vec<i64> {
    let r = &m.representatives;
    let mut all: Vec<i64> = vec![
        r.heavy,
        r.median,
        r.light,
        r.high_overfetch,
        r.low_overfetch,
    ];
    all.extend(&r.sample);
    all.sort_unstable();
    all.dedup();
    all
}

/// Upload the objects and register them through the product's ordinary write path.
///
/// The metadata is built by `ukiel_core::stats` — the same builders ingest and
/// compaction use — from the **actual Arrow batches read back out of the generated
/// files**. Not from the topology, and not from the manifest.
///
/// That indirection is the whole point. If the loader took the key set from the
/// topology, it would be asserting that the file contains what the graph says it
/// contains, which is exactly the claim under test. Reading it back means the roaring
/// bitmap, and therefore the catalog's issue-0014 Bloom filter, is derived from the
/// bytes that are actually in the object store. If the generator ever wrote a file
/// that disagreed with its own graph, this is where it surfaces.
pub async fn load_materialized(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    manifest: &Manifest,
    topology: &Topology,
    dir: &Path,
    label: &str,
    hypertable_id: HypertableId,
) -> Result<Vec<PartMeta>> {
    let prefix = object_prefix(label);
    let mut metas = Vec::with_capacity(manifest.parts.len());

    for part in &manifest.parts {
        let local = dir.join(&part.path);
        let bytes =
            std::fs::read(&local).with_context(|| format!("reading {}", local.display()))?;

        // The artifact's digest, re-checked at the moment of upload. Between `verify`
        // and here, the file could have been replaced; the object store is about to
        // hold these bytes for as long as the fixture lives.
        let digest = prod_synth_contract::digest_bytes(&bytes);
        if digest != part.digest {
            bail!(
                "{}: digest changed between verification and upload (expected {}, got {digest})",
                part.path,
                part.digest
            );
        }

        let key = format!("{prefix}/{}", part.path);
        let store_path = StorePath::from(key.clone());

        // Write-ahead upload intent, exactly as a product writer does it: the row is
        // recorded before the object exists, and the commit that references the object
        // deletes it. An upload that is never committed is then a discoverable orphan
        // rather than a mystery object nobody will ever find.
        catalog
            .register_pending_objects(hypertable_id, std::slice::from_ref(&key))
            .await
            .with_context(|| format!("recording upload intent for {key}"))?;

        store
            .put(&store_path, bytes.clone().into())
            .await
            .with_context(|| format!("uploading {key}"))?;

        // The object's real size, from the store — not from the manifest, and not from
        // what we meant to upload.
        let head = store
            .head(&store_path)
            .await
            .with_context(|| format!("HEAD {key}"))?;
        if head.size != bytes.len() as u64 {
            bail!(
                "{key}: uploaded {} bytes, the store reports {}",
                bytes.len(),
                head.size
            );
        }

        let meta = part_meta(&key, &bytes, part, head.size as i64, manifest)?;
        metas.push(meta);
    }

    // One commit for the whole fixture: it is one logical event, and a fixture that
    // is half-committed is not a fixture.
    catalog
        .commit(
            hypertable_id,
            CommitOp::Add {
                parts: metas.clone(),
            },
            None,
        )
        .await
        .context("committing the fixture's parts")?;

    let _ = topology;
    Ok(metas)
}

/// Build one part's catalog metadata from the file's own bytes, with the product's
/// own stats builders.
fn part_meta(
    key: &str,
    bytes: &[u8],
    part: &prod_synth_contract::ManifestPart,
    size_bytes: i64,
    manifest: &Manifest,
) -> Result<PartMeta> {
    let packing_key = &manifest.table.packing_key;

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .with_context(|| format!("opening {key}"))?;
    let parquet_meta = reader.metadata().clone();
    let reader = reader.build().with_context(|| format!("reading {key}"))?;

    // Fold every batch through the product's accumulators — the same ones the
    // streaming compactor uses, so the JSON shape cannot drift between a fixture and
    // a real merge output.
    let mut stats = ukiel_core::stats::Int64StatsAccumulator::default();
    let mut keys = ukiel_core::stats::KeyBitmapAccumulator::default();
    let mut rows = 0i64;
    let mut key_min = i64::MAX;
    let mut key_max = i64::MIN;

    for batch in reader {
        let batch: RecordBatch = batch.with_context(|| format!("decoding {key}"))?;
        rows += batch.num_rows() as i64;
        stats.update(&batch);
        keys.update(&batch, packing_key);

        let idx = batch
            .schema()
            .index_of(packing_key)
            .map_err(|_| anyhow::anyhow!("{key}: no '{packing_key}' column"))?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("{key}: '{packing_key}' is not int64"))?;
        if let Some(v) = arrow::compute::min(col) {
            key_min = key_min.min(v);
        }
        if let Some(v) = arrow::compute::max(col) {
            key_max = key_max.max(v);
        }
    }

    if rows != part.rows as i64 {
        bail!(
            "{key}: the file holds {rows} rows, the manifest says {}",
            part.rows
        );
    }
    if key_min != part.key_min || key_max != part.key_max {
        bail!(
            "{key}: the file's key range is [{key_min}, {key_max}], the manifest says [{}, {}]",
            part.key_min,
            part.key_max
        );
    }

    // The key index, gated exactly as every product writer gates it: multi-key parts
    // only. A single-key file's `packing_key_min == packing_key_max` already *is* its
    // key set, so the range predicate is exact for it and a bitmap would be overhead.
    let multi_key = key_min != key_max;
    let bitmap = multi_key.then(|| keys.finish()).flatten();
    let spans = ukiel_core::stats::key_row_groups(parquet_meta.row_groups(), packing_key);
    let column_stats = ukiel_core::stats::with_key_index(stats.finish(), bitmap, spans);

    Ok(PartMeta {
        path: key.to_string(),
        // Provenance, and labelled as such in the hypertable's partition spec.
        partition_values: serde_json::json!({
            "source_partition": part.provenance.source_partition_id.to_string()
        }),
        packing_key_min: key_min,
        packing_key_max: key_max,
        row_count: rows,
        // The object's real size, from the store's own HEAD.
        size_bytes,
        // A fixed non-L0 level, so an idle benchmark is stable. NOT the source's
        // ClickHouse merge level, which is provenance and is never mapped onto Ukiel's
        // compaction ladder.
        level: 1,
        column_stats,
    })
}

/// Per representative tenant, the three counts side by side.
pub async fn measure_geometry(
    catalog: &PostgresCatalog,
    hypertable_id: HypertableId,
    manifest: &Manifest,
    topology: &Topology,
) -> Result<Vec<TenantGeometry>> {
    // The exact truth, from the graph.
    let mut exact: BTreeMap<i64, u64> = BTreeMap::new();
    for (tenant, _part, _rows) in topology.edges() {
        *exact.entry(tenant).or_default() += 1;
    }
    let rows_of: BTreeMap<i64, u64> = topology.tenants.iter().map(|t| (t.id, t.rows)).collect();

    // What the range index alone would return: the parts whose declared key range
    // brackets the tenant.
    let ranges: Vec<(i64, i64)> = manifest
        .parts
        .iter()
        .map(|p| (p.key_min, p.key_max))
        .collect();

    let r = &manifest.representatives;
    let classes = [
        (r.heavy, "heavy"),
        (r.median, "median"),
        (r.light, "light"),
        (r.high_overfetch, "high_overfetch"),
        (r.low_overfetch, "low_overfetch"),
    ];

    let mut out = Vec::new();
    for (tenant, class) in classes {
        let range = ranges
            .iter()
            .filter(|(lo, hi)| *lo <= tenant && tenant <= *hi)
            .count() as u64;

        // What the catalog actually ships, once the issue-0014 key filter has rejected
        // the parts it can prove do not hold the tenant.
        let shipped = catalog
            .live_parts_pruned(hypertable_id, Some(tenant), &[])
            .await
            .with_context(|| format!("pruned lookup for tenant {tenant}"))?
            .len() as u64;

        let exact_n = *exact.get(&tenant).unwrap_or(&0);

        // The one-directional guarantee. The filter may keep a part it could have
        // skipped; it may NEVER drop one that holds the tenant. A single false negative
        // here is a row that has silently vanished from a query result.
        if shipped < exact_n {
            bail!(
                "tenant {tenant}: the catalog shipped {shipped} parts but the tenant is really in \
                 {exact_n}. The key filter has produced a FALSE NEGATIVE — rows would be missing \
                 from query results with nothing in any log."
            );
        }

        out.push(TenantGeometry {
            tenant,
            class: class.to_string(),
            exact: exact_n,
            range,
            shipped,
            rows: *rows_of.get(&tenant).unwrap_or(&0),
        });
    }
    Ok(out)
}

/// Every member of every part is returned for its own key: zero false negatives,
/// checked across the whole deterministic tenant sample rather than the five
/// representatives.
pub async fn assert_no_false_negatives(
    catalog: &PostgresCatalog,
    hypertable_id: HypertableId,
    manifest: &Manifest,
    topology: &Topology,
) -> Result<u64> {
    let mut membership: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
    for (tenant, part, _rows) in topology.edges() {
        membership.entry(tenant).or_default().push(part);
    }

    // The manifest's part index, by object path, so a returned part can be identified.
    let index_of: BTreeMap<String, u32> = manifest
        .parts
        .iter()
        .map(|p| (p.path.clone(), p.index))
        .collect();

    let mut checked = 0u64;
    for tenant in &manifest.representatives.sample {
        let want: std::collections::BTreeSet<u32> = membership
            .get(tenant)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();

        let got = catalog
            .live_parts_pruned(hypertable_id, Some(*tenant), &[])
            .await
            .with_context(|| format!("pruned lookup for tenant {tenant}"))?;

        let got_idx: std::collections::BTreeSet<u32> = got
            .iter()
            .filter_map(|p| {
                // Object keys are prefixed with the label; the manifest's are relative.
                index_of
                    .iter()
                    .find(|(rel, _)| p.meta.path.ends_with(rel.as_str()))
                    .map(|(_, i)| *i)
            })
            .collect();

        let missing: Vec<u32> = want.difference(&got_idx).copied().collect();
        if !missing.is_empty() {
            bail!(
                "tenant {tenant} is a member of parts {missing:?}, and the catalog did not return \
                 them. Pruning may only ever remove a part it can PROVE holds no matching rows."
            );
        }
        checked += 1;
    }
    Ok(checked)
}
