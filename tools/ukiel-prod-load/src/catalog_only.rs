//! The catalog-only arm: the same truthful topology, no objects.
//!
//! It exists because the two questions have wildly different costs. "How many parts
//! does the catalog ship for this tenant, and how many does the key filter reject?"
//! needs the membership graph and nothing else, and answering it should take seconds.
//! "How long does the scan take, and how many bytes does it read?" needs real files,
//! and there is no way to make that cheap. One fixture cannot be both, and a fake path
//! cannot answer the second question — so this arm answers the first, honestly, and
//! declines the second out loud.
//!
//! What stays truthful here: the parts, their key ranges, their exact member sets, and
//! therefore the roaring bitmaps and the issue-0014 key filters the catalog derives
//! from them. Those *are* the thing under measurement.
//!
//! What is deliberately fake, and cannot be mistaken for real: `size_bytes = 0`, and
//! `catalog-only://` paths that no object store can open. A benchmark that tried to
//! read one would fail immediately and audibly, rather than quietly returning an empty
//! part and calling it a fast scan.
//!
//! ## Why this arm insists on a disposable stack
//!
//! These part rows point at objects that do not exist. A compactor that picks one up
//! will try to merge a file that was never written; a GC sweeper will try to reap it.
//! Neither failure is *dangerous*, but both are confusing, and the confusion would land
//! on whoever is next to look at that deployment. So the command refuses to run against
//! a catalog whose configuration gives anything else a compactor or GC role, cleans up
//! after itself on success, attempts the same cleanup on failure, and — if cleanup
//! itself fails — prints the exact SQL to finish the job by hand.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use prod_synth_contract::{Manifest, Topology};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, HypertableId, PartMeta};

use crate::{CATALOG_ONLY_SCHEME, hypertable_name};

/// Build the part rows from the topology alone.
///
/// The key set here comes from the graph rather than from a file, and that is sound
/// *precisely because* there is no file: there is nothing for it to disagree with. The
/// materialized arm reads its keys back out of the Parquet, because there the file is
/// the authority and the graph is the claim under test.
pub fn part_rows(manifest: &Manifest, topology: &Topology) -> Result<Vec<PartMeta>> {
    let mut members: BTreeMap<u32, &prod_synth_contract::Membership> = BTreeMap::new();
    for s in &topology.shards {
        for m in &s.memberships {
            members.insert(m.part, m);
        }
    }

    let mut out = Vec::with_capacity(manifest.parts.len());
    for part in &manifest.parts {
        let m = members
            .get(&part.index)
            .ok_or_else(|| anyhow::anyhow!("part {} is not in the topology", part.index))?;

        let keys: Vec<i64> = m.tenants.clone();
        let key_min = *keys.first().expect("every part has a member");
        let key_max = *keys.last().expect("every part has a member");
        if key_min != part.key_min || key_max != part.key_max {
            bail!(
                "part {}: the topology's members span [{key_min}, {key_max}] but the manifest \
                 declares [{}, {}]",
                part.index,
                part.key_min,
                part.key_max
            );
        }

        // The plan-16 roaring bitmap, built with the product's own encoder from the
        // graph's exact key set — so the catalog derives exactly the issue-0014 key
        // filter it would derive from a real file with these keys. Multi-key parts
        // only, the same gate every product writer applies.
        let bitmap = (key_min != key_max).then(|| bitmap_of(&keys)).flatten();

        let column_stats = ukiel_core::stats::with_key_index(
            Some(serde_json::json!({
                manifest.table.packing_key.clone(): {"min": key_min, "max": key_max}
            })),
            bitmap,
            None,
        );

        out.push(PartMeta {
            // Un-fetchable by construction. If anything ever tries to open this, it must
            // fail loudly — never fall back to an empty read that would look like an
            // empty part.
            path: format!(
                "{CATALOG_ONLY_SCHEME}{}/{}",
                manifest.config.seed, part.path
            ),
            partition_values: serde_json::json!({
                "source_partition": part.provenance.source_partition_id.to_string()
            }),
            packing_key_min: key_min,
            packing_key_max: key_max,
            // The rows are real — the graph says so, and nothing about a row count needs
            // a file to be true.
            row_count: m.rows.iter().sum::<u64>() as i64,
            // Zero, and honestly zero. This arm measures no object behaviour, so it
            // records no object size rather than inventing a plausible one.
            size_bytes: 0,
            level: 1,
            column_stats,
        });
    }
    Ok(out)
}

/// The plan-16 roaring treemap, base64, via the product's own accumulator path.
///
/// `KeyBitmapAccumulator` folds Arrow batches, and there is no batch here — so this
/// builds the same encoding from the key set directly. It stays honest by construction:
/// the caps and the encoding are `ukiel_core`'s, and the round-trip is asserted, so a
/// bitmap this produces decodes to exactly the keys that went in.
fn bitmap_of(keys: &[i64]) -> Option<String> {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // A one-column batch of the key set, handed to the *product's* accumulator. Building
    // the roaring bytes here by hand would be a second implementation of the encoding,
    // and the whole point is that the catalog derives its filter from the same bytes it
    // would derive from a real writer's output.
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch =
        arrow::array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys.to_vec()))])
            .ok()?;

    let mut acc = ukiel_core::stats::KeyBitmapAccumulator::default();
    acc.update(&batch, "k");
    acc.finish()
}

/// Seed the catalog. One hypertable, one commit, no objects.
pub async fn seed(
    catalog: &PostgresCatalog,
    manifest: &Manifest,
    topology: &Topology,
    label: &str,
) -> Result<(HypertableId, Vec<PartMeta>)> {
    let id = crate::load::create_hypertable(catalog, manifest, label).await?;
    let parts = part_rows(manifest, topology)?;

    catalog
        .commit(
            id,
            CommitOp::Add {
                parts: parts.clone(),
            },
            None,
        )
        .await
        .context("seeding the catalog-only parts")?;

    Ok((id, parts))
}

/// Refuse to run anywhere a background worker could pick up a part that has no object.
///
/// Not a safety rail against data loss — there is no data here to lose. It is a rail
/// against *confusion*: a compactor that fails to open `catalog-only://…` produces an
/// error that will land on whoever is next to look at that deployment, and they will
/// have no idea what it is.
///
/// Typed against `ukield`'s own `Role` enum rather than strings, so this cannot be
/// defeated by a config that spells a role differently than this tool expected.
pub fn require_disposable(roles: &[ukield::config::Role]) -> Result<()> {
    use ukield::config::Role;
    let dangerous: Vec<&Role> = roles
        .iter()
        .filter(|r| matches!(r, Role::Compactor | Role::Gc))
        .collect();
    if !dangerous.is_empty() {
        bail!(
            "this configuration gives ukield the {dangerous:?} role(s). The catalog-only arm seeds \
             part rows whose objects do not exist: a compactor would try to merge them and a GC \
             sweeper would try to reap them, and the resulting errors would land on whoever next \
             looks at this deployment. Point --config at a disposable stack with those roles off."
        );
    }
    Ok(())
}

/// Remove everything this arm created. Called on success, attempted on failure.
///
/// Returns the manual reset command if it could not finish, so a failed cleanup is
/// never a shrug — it is an instruction.
pub async fn cleanup(catalog: &PostgresCatalog, label: &str) -> Result<(), String> {
    let name = hypertable_name(label);
    let pool = catalog.pool_for_tests();

    // Parts, then commits, then the tables — foreign keys run the other way.
    let sql = [
        "DELETE FROM parts WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM pending_objects WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM commits WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM logical_tables WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM compaction_leases WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM worker_cursors WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM ingest_offsets WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = $1)",
        "DELETE FROM hypertables WHERE name = $1",
    ];

    for stmt in sql {
        if let Err(e) = sqlx::query(stmt).bind(&name).execute(pool).await {
            return Err(format!(
                "cleanup failed: {e}\n\nFinish it by hand:\n{}",
                manual_reset(&name)
            ));
        }
    }
    Ok(())
}

/// The exact SQL to remove a catalog-only fixture, printed whenever cleanup fails.
///
/// Never leave fake paths lying around for a background compactor to find, and never
/// leave an operator guessing which tables to touch.
pub fn manual_reset(hypertable: &str) -> String {
    format!(
        "  psql \"$UKIEL_CATALOG_URL\" <<'SQL'\n\
         \x20 BEGIN;\n\
         \x20 DELETE FROM parts             WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM pending_objects   WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM commits           WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM logical_tables    WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM compaction_leases WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM worker_cursors    WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM ingest_offsets    WHERE hypertable_id IN (SELECT id FROM hypertables WHERE name = '{hypertable}');\n\
         \x20 DELETE FROM hypertables       WHERE name = '{hypertable}';\n\
         \x20 COMMIT;\n\
         SQL"
    )
}
