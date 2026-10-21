use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use ukiel_pipeline::{ExecutionLimits, PipelineError, execute_pipeline, prepare_pipeline};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Int64,
        false,
    )]))
}

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

#[tokio::test]
async fn clocks_random_values_and_processing_time_predicates_execute() {
    let prepared = prepare_pipeline(
        "events",
        "SELECT amount, now() AS inserted_at, random() AS draw FROM events WHERE now() > TIMESTAMP '2020-01-01'",
        schema(),
        ExecutionLimits::default(),
    ).await.unwrap();
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let output = execute_pipeline(&prepared, vec![batch(vec![1, 2])])
        .await
        .unwrap();
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in &output {
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let draws = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(times.values().iter().all(|x| (before..=after).contains(x)));
        assert!(draws.values().iter().all(|x| (0.0..1.0).contains(x)));
    }
}

#[tokio::test]
async fn all_input_batches_form_one_sql_execution() {
    let prepared = prepare_pipeline(
        "events",
        "SELECT SUM(amount) AS total FROM events HAVING SUM(amount) > 2",
        schema(),
        ExecutionLimits::default(),
    )
    .await
    .unwrap();
    for input in [
        vec![batch(vec![1]), batch(vec![2])],
        vec![batch(vec![1, 2])],
    ] {
        let output = execute_pipeline(&prepared, input).await.unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let total = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 3);
    }
}

#[tokio::test]
async fn unordered_limits_and_order_sensitive_aggregates_are_allowed() {
    for sql in [
        "SELECT amount FROM events LIMIT 1 OFFSET 1",
        "SELECT array_agg(amount) FROM events",
        "SELECT amount, row_number() OVER () FROM events",
    ] {
        let prepared = prepare_pipeline("events", sql, schema(), ExecutionLimits::default())
            .await
            .unwrap();
        let output = execute_pipeline(&prepared, vec![batch(vec![1, 2, 3])])
            .await
            .unwrap();
        assert!(output.iter().any(|batch| batch.num_rows() > 0));
    }
}

#[tokio::test]
async fn prepared_pipeline_uses_fresh_input_and_keeps_empty_output_schema() {
    let prepared = prepare_pipeline(
        "events",
        "SELECT amount FROM events WHERE amount > 1",
        schema(),
        ExecutionLimits::default(),
    )
    .await
    .unwrap();
    let empty = execute_pipeline(&prepared, vec![batch(vec![1])])
        .await
        .unwrap();
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(prepared.output_schema().field(0).name(), "amount");
    let rows = execute_pipeline(&prepared, vec![batch(vec![2])])
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn query_scope_rejects_mutations_extra_scans_and_hidden_subquery_scans() {
    for sql in [
        "SELECT now()",
        "SELECT a.amount FROM events a JOIN events b ON a.amount = b.amount",
        "SELECT * FROM events, events AS b",
        "SELECT amount, (SELECT max(amount) FROM events) FROM events",
        "CREATE TABLE another AS SELECT * FROM events",
        "INSERT INTO events SELECT * FROM events",
        "DROP TABLE events",
        "SELECT * FROM events; SELECT * FROM events",
    ] {
        assert!(
            prepare_pipeline("events", sql, schema(), ExecutionLimits::default())
                .await
                .is_err(),
            "accepted {sql}"
        );
    }
}

#[tokio::test]
async fn input_schema_and_whole_execution_budgets_are_enforced() {
    let limits = ExecutionLimits::default();
    for (limited, resource) in [
        (
            ExecutionLimits {
                input_bytes: 1,
                ..limits
            },
            "input bytes",
        ),
        (
            ExecutionLimits {
                output_bytes: 1,
                ..limits
            },
            "output bytes",
        ),
        (
            ExecutionLimits {
                output_rows: 1,
                ..limits
            },
            "output rows",
        ),
    ] {
        let prepared = prepare_pipeline("events", "SELECT amount FROM events", schema(), limited)
            .await
            .unwrap();
        let error = execute_pipeline(&prepared, vec![batch(vec![1]), batch(vec![2])])
            .await
            .unwrap_err();
        assert!(
            matches!(error, PipelineError::BudgetExceeded { resource: r, .. } if r == resource),
            "{error}"
        );
    }
    let prepared = prepare_pipeline("events", "SELECT amount FROM events", schema(), limits)
        .await
        .unwrap();
    let wrong = RecordBatch::new_empty(Arc::new(Schema::empty()));
    assert!(matches!(
        execute_pipeline(&prepared, vec![wrong]).await,
        Err(PipelineError::InputSchemaMismatch)
    ));
}
