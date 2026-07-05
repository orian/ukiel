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

pub fn arrow_schema_from_json(v: &serde_json::Value) -> Result<Schema, SchemaError> {
    let fields = v
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| SchemaError::Invalid("missing 'fields' array".to_string()))?;

    let mut arrow_fields = Vec::with_capacity(fields.len());
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
        arrow_fields.push(Field::new(name, data_type, nullable));
    }
    Ok(Schema::new(arrow_fields))
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
}
