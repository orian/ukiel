//! The generated event table: a supported subset of the captured ClickHouse
//! schema.
//!
//! What is adapted, and why — because each of these is a place a reader could
//! otherwise assume a fidelity the fixture does not have:
//!
//! * **`properties` is `utf8`, not a JSON type.** That is the physical reality of
//!   the captured ClickHouse `String` column, and it is also all Ukiel supports
//!   today. The values are valid compact JSON, so a future JSON plan can add
//!   extraction queries over the very same bytes without regenerating anything.
//!
//! * **The sort key uses columns, not expressions.** ClickHouse sorts by
//!   `team_id, toDate(timestamp), event, cityHash64(distinct_id), cityHash64(uuid)`.
//!   Ukiel declares *columns*, not arbitrary sort expressions, so the hashes become
//!   direct string ordering on `distinct_id` and `uuid`. The packing key stays
//!   first, which is the property that actually matters: it is what makes a part's
//!   rows for one tenant contiguous.
//!
//! * **The `mat_*` columns are stored, not materialized expressions.** Ukiel can
//!   express a materialized column, but its expression language is not ClickHouse's,
//!   and the point of these columns here is to be *promoted copies* of three JSON
//!   properties — which the generator writes directly and identically. Declaring them
//!   `Materialized` would hand the same job to an expression evaluator for no gain
//!   and one more thing to drift.
//!
//! Types the capture uses and Ukiel does not have — `Map`, `Array`, `Enum`, `UUID`,
//! `DateTime64`, `LowCardinality` — are simply not generated. The fixture is a subset
//! and says so.

use prod_synth_contract::TableSpec;
use serde_json::json;

pub const PACKING_KEY: &str = "team_id";
pub const TS_COLUMN: &str = "timestamp";

pub fn table_spec() -> TableSpec {
    TableSpec {
        packing_key: PACKING_KEY.to_string(),
        sort_key: vec![
            PACKING_KEY.to_string(),
            TS_COLUMN.to_string(),
            "event".to_string(),
            "distinct_id".to_string(),
            "uuid".to_string(),
        ],
        ts_column: TS_COLUMN.to_string(),
        schema: json!({
            "fields": [
                {"name": "team_id",          "type": "int64",        "nullable": false},
                {"name": "timestamp",        "type": "timestamp_ms", "nullable": false},
                {"name": "event",            "type": "utf8",         "nullable": false},
                {"name": "distinct_id",      "type": "utf8",         "nullable": false},
                {"name": "uuid",             "type": "utf8",         "nullable": false},
                // Valid compact JSON text. See the module note: `utf8` is the
                // captured column's physical type, not a placeholder for one.
                {"name": "properties",       "type": "utf8",         "nullable": false},
                {"name": "elements_chain",   "type": "utf8",         "nullable": false},
                // Promoted from `properties`. A query reading these and a query
                // digging the same keys out of the JSON must agree.
                {"name": "mat_$current_url", "type": "utf8",         "nullable": false, "bloom_filter": true},
                {"name": "mat_$host",        "type": "utf8",         "nullable": false},
                {"name": "mat_$lib",         "type": "utf8",         "nullable": false}
            ]
        }),
    }
}
