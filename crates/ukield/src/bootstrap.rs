//! Idempotent table bootstrap: applies the config's [[tables]] on every boot.
//! Creates what is missing; never mutates what exists (v1 has no ALTER).

use anyhow::Context;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::{NamespaceId, Placement};

use crate::config::TableConfig;

pub async fn apply(catalog: &PostgresCatalog, tables: &[TableConfig]) -> anyhow::Result<()> {
    for table in tables {
        let schema = table.schema_json();
        ukiel_expr::validate_table_schema(&schema)
            .with_context(|| format!("invalid schema for table '{}'", table.name))?;

        // Sort-order invariants (plan 27), re-checked on every boot so a config
        // that predates the validation fails fast rather than silently letting
        // ingest write files the query provider's declared ordering doesn't
        // match. Covers the ts_column rule the catalog can't (ts lives here).
        let physical = ukiel_core::TableColumns::parse(&schema)
            .with_context(|| format!("parsing schema for table '{}'", table.name))?
            .physical_schema();
        ukiel_core::validate_sort_key(
            &physical,
            &table.sort_key,
            &table.packing_key,
            Some(&table.ts_column),
        )
        .with_context(|| format!("invalid sort_key for table '{}'", table.name))?;

        let hypertable = match catalog.get_hypertable(&table.name).await {
            Ok(existing) => {
                if existing.table_schema != schema {
                    tracing::warn!(
                        table = %table.name,
                        "config schema differs from existing hypertable; \
                         not altering (v1 has no ALTER) — using the existing schema"
                    );
                }
                existing
            }
            Err(CatalogError::NotFound(_)) => {
                let id = catalog
                    .create_hypertable(
                        &table.name,
                        &schema,
                        &table.partition_spec(),
                        &table.sort_key,
                        &table.packing_key,
                    )
                    .await
                    .with_context(|| format!("creating hypertable '{}'", table.name))?;
                let placement = placement_from(table)?;
                if placement != Placement::Packed {
                    catalog.set_placement(id, placement).await?;
                }
                tracing::info!(table = %table.name, id = %id, "created hypertable");
                catalog.get_hypertable(&table.name).await?
            }
            Err(e) => return Err(e.into()),
        };

        for &ns in &table.namespaces {
            match catalog
                .get_logical_table(NamespaceId(ns), &table.name)
                .await
            {
                Ok(_) => {}
                Err(CatalogError::NotFound(_)) => {
                    catalog
                        .create_logical_table(NamespaceId(ns), &table.name, hypertable.id)
                        .await
                        .with_context(|| {
                            format!("creating logical table '{}' for namespace {ns}", table.name)
                        })?;
                    tracing::info!(table = %table.name, namespace = ns, "created logical table");
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

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
