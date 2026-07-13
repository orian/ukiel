//! The query path's string representation: `Utf8View` instead of `Utf8`.
//!
//! **This is parity with the engine's own default, not a departure from it.**
//! DataFusion 54 ships `schema_force_view_types = true`
//! (`datafusion-common-54.0.0/src/config.rs:809`), so a stock deployment reads
//! Parquet string columns as `Utf8View` — zero-copy views over the decode
//! buffer, which then flow through repartitioning and aggregation without ever
//! being copied. Ukiel's provider used to hand `ParquetSource` an explicit
//! `Utf8`-typed schema, which silently *opted out*: `arrow_typeof("URL")`
//! returned `Utf8` on ukiel and `Utf8View` on raw DataFusion **over the same
//! files**. Measured cost (plan 37's H6, gap note): +62% peak memory, +218%
//! repartition time, 1.53× wall on a byte-identical scan.
//!
//! **The boundary is the crate line, and it is deliberate.** This mapping is
//! applied *only* to the two schemas the query provider hands the reader.
//! `TableColumns::physical_schema()`/`query_schema()` in `ukiel-core` stay
//! `Utf8` — ingest builds owned `StringArray`s from JSON, the compactor's merge
//! reads and rewrites with the same schema, and materialized columns evaluate at
//! write time. None of that changes, and none of it needs to: view-vs-owned is
//! an **in-memory representation**, not a storage format. The Parquet bytes are
//! byte-identical either way.

use datafusion::arrow::datatypes::{DataType, Field, Schema};

/// Every `Utf8` field becomes `Utf8View`; everything else passes through
/// untouched, preserving name, nullability, metadata, and field order.
///
/// Order and nullability are load-bearing, not incidental: the provider projects
/// by index (`TableColumns.specs` is index-aligned with the query schema) and
/// DataFusion validates the scan's declared ordering against this schema.
///
/// The two provider schemas must be mapped **together**. Flipping only the scan
/// schema would leave SQL seeing `Utf8`, and DataFusion would insert a cast that
/// re-materializes every string — handing the entire win straight back.
pub fn view_typed(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 => Field::new(f.name(), DataType::Utf8View, f.is_nullable())
                .with_metadata(f.metadata().clone()),
            _ => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn utf8_becomes_view_and_nothing_else_moves() {
        let schema = Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            // `timestamp_ms` is physically Int64 — it must not be touched.
            Field::new("ts", DataType::Int64, true).with_metadata(HashMap::from([(
                "ukiel_type".to_string(),
                "timestamp_ms".to_string(),
            )])),
            Field::new("payload", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, true),
        ]);
        let viewed = view_typed(&schema);

        // Only the utf8 column changes representation.
        assert_eq!(viewed.field(2).data_type(), &DataType::Utf8View);
        for i in [0usize, 1, 3, 4] {
            assert_eq!(
                viewed.field(i).data_type(),
                schema.field(i).data_type(),
                "field {i} must pass through untouched"
            );
        }

        // Names, order and nullability are load-bearing: the provider projects by
        // index and DataFusion validates the declared ordering against this schema.
        let names: Vec<&str> = viewed.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["tenant_id", "ts", "payload", "amount", "flag"]);
        for i in 0..schema.fields().len() {
            assert_eq!(viewed.field(i).is_nullable(), schema.field(i).is_nullable());
        }
        // Metadata survives — the `ukiel_type` hint distinguishes timestamp_ms
        // from a plain int64 and must not be dropped in passing.
        assert_eq!(viewed.field(1).metadata(), schema.field(1).metadata());
    }

    #[test]
    fn utf8_metadata_survives_the_remap() {
        let schema = Schema::new(vec![
            Field::new("url", DataType::Utf8, false)
                .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
        ]);
        let viewed = view_typed(&schema);
        assert_eq!(viewed.field(0).data_type(), &DataType::Utf8View);
        assert!(!viewed.field(0).is_nullable());
        assert_eq!(viewed.field(0).metadata()["k"], "v");
    }

    #[test]
    fn empty_schema_is_empty() {
        assert_eq!(view_typed(&Schema::empty()).fields().len(), 0);
    }
}
