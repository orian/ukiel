//! Mapping from the catalog's JSON table schema to an Arrow schema.
//!
//! Supported field types: `int64`, `float64`, `utf8`, `bool`, `timestamp_ms`.
//! v1 maps `timestamp_ms` to Arrow `Int64` (epoch milliseconds).

use arrow::datatypes::{DataType, Field, Schema};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("invalid table schema: {0}")]
    Invalid(String),
    #[error("unsupported field type '{0}'")]
    UnsupportedType(String),
}

/// How a column gets its value. Expressions are DataFusion SQL strings over
/// this table's stored/default columns; they must be deterministic (enforced
/// by `ukiel-expr` at compile time).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    /// Plain column: value comes from the insert.
    Stored,
    /// Value comes from the insert; NULL/missing slots are filled with the
    /// expression at ingest. Stored.
    Default(String),
    /// Always computed from other columns at write time (insert values are
    /// ignored) and recomputed on every rewrite. Stored.
    Materialized(String),
    /// Computed at query time. Never stored.
    Alias(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumns {
    pub specs: Vec<ColumnSpec>,
}

impl TableColumns {
    pub fn parse(v: &serde_json::Value) -> Result<Self, SchemaError> {
        let fields = v
            .get("fields")
            .and_then(|f| f.as_array())
            .ok_or_else(|| SchemaError::Invalid("missing 'fields' array".to_string()))?;

        let mut specs = Vec::with_capacity(fields.len());
        for f in fields {
            let name = f
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| SchemaError::Invalid("field missing 'name'".to_string()))?;
            let type_name = f
                .get("type")
                .and_then(|t| t.as_str())
                .ok_or_else(|| SchemaError::Invalid(format!("field '{name}' missing 'type'")))?;
            let nullable = f.get("nullable").and_then(|n| n.as_bool()).unwrap_or(true);

            let data_type = match type_name {
                "int64" | "timestamp_ms" => DataType::Int64,
                "float64" => DataType::Float64,
                "utf8" => DataType::Utf8,
                "bool" => DataType::Boolean,
                other => return Err(SchemaError::UnsupportedType(other.to_string())),
            };

            let expr_of = |key: &str| f.get(key).and_then(|e| e.as_str()).map(String::from);
            let kind = match (
                expr_of("default"),
                expr_of("materialized"),
                expr_of("alias"),
            ) {
                (None, None, None) => ColumnKind::Stored,
                (Some(e), None, None) => ColumnKind::Default(e),
                (None, Some(e), None) => ColumnKind::Materialized(e),
                (None, None, Some(e)) => ColumnKind::Alias(e),
                _ => {
                    return Err(SchemaError::Invalid(format!(
                        "field '{name}': default/materialized/alias are mutually exclusive"
                    )));
                }
            };
            specs.push(ColumnSpec {
                name: name.to_string(),
                data_type,
                nullable,
                kind,
            });
        }
        Ok(TableColumns { specs })
    }

    fn schema_where(&self, include: impl Fn(&ColumnKind) -> bool) -> Schema {
        Schema::new(
            self.specs
                .iter()
                .filter(|s| include(&s.kind))
                .map(|s| Field::new(&s.name, s.data_type.clone(), s.nullable))
                .collect::<Vec<_>>(),
        )
    }

    /// What ingest rows may carry: stored + default columns.
    pub fn input_schema(&self) -> Schema {
        self.schema_where(|k| matches!(k, ColumnKind::Stored | ColumnKind::Default(_)))
    }

    /// What Parquet files contain: stored + default + materialized.
    pub fn physical_schema(&self) -> Schema {
        self.schema_where(|k| !matches!(k, ColumnKind::Alias(_)))
    }

    /// What SQL sees: physical + alias.
    pub fn query_schema(&self) -> Schema {
        self.schema_where(|_| true)
    }
}

pub fn arrow_schema_from_json(v: &serde_json::Value) -> Result<Schema, SchemaError> {
    Ok(TableColumns::parse(v)?.physical_schema())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_all_supported_types() {
        let schema = arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "value", "type": "float64"},
            {"name": "payload", "type": "utf8", "nullable": true},
            {"name": "flag", "type": "bool", "nullable": false}
        ]}))
        .unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64); // timestamp_ms -> Int64 in v1
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        assert!(schema.field(0).is_nullable()); // default nullable
        assert!(!schema.field(4).is_nullable());
    }

    #[test]
    fn rejects_unknown_type_and_malformed_input() {
        let err = arrow_schema_from_json(&json!({"fields": [{"name": "x", "type": "decimal"}]}))
            .unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedType(_)));

        let err = arrow_schema_from_json(&json!({"no_fields": true})).unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)));
    }

    fn rich_schema() -> serde_json::Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "amount", "type": "float64", "default": "0.0"},
            {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"},
            {"name": "half", "type": "float64", "alias": "amount / 2.0"}
        ]})
    }

    #[test]
    fn parses_column_kinds_and_derives_schemas() {
        let cols = TableColumns::parse(&rich_schema()).unwrap();
        assert!(matches!(cols.specs[0].kind, ColumnKind::Stored));
        assert!(matches!(cols.specs[2].kind, ColumnKind::Default(ref e) if e == "0.0"));
        assert!(matches!(cols.specs[3].kind, ColumnKind::Materialized(_)));
        assert!(matches!(cols.specs[4].kind, ColumnKind::Alias(_)));

        let names = |s: &Schema| {
            s.fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(&cols.input_schema()),
            vec!["tenant_id", "ts", "amount"]
        );
        assert_eq!(
            names(&cols.physical_schema()),
            vec!["tenant_id", "ts", "amount", "doubled"]
        );
        assert_eq!(
            names(&cols.query_schema()),
            vec!["tenant_id", "ts", "amount", "doubled", "half"]
        );

        // arrow_schema_from_json stays the physical schema.
        assert_eq!(
            arrow_schema_from_json(&rich_schema()).unwrap(),
            cols.physical_schema()
        );
    }

    #[test]
    fn rejects_conflicting_kind_attributes() {
        let err = TableColumns::parse(&json!({"fields": [
            {"name": "x", "type": "int64", "default": "1", "materialized": "2"}
        ]}))
        .unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)));
    }
}
