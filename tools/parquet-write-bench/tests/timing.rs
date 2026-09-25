//! The clock brackets exactly the write: work moved *before* the clock (decode, projection,
//! fingerprinting) or *after* it (validation, report writing) does not inflate the measured
//! interval, while more work *inside* the write does grow it.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet_lab_write_core::{WriterConfig, sorting_columns, write_prepared};
use parquet_write_bench::measure;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn batch(n: usize) -> RecordBatch {
    let k: Vec<i64> = (0..n as i64).collect();
    let s: Vec<Option<String>> = (0..n).map(|i| Some(format!("row-{i}"))).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(StringArray::from(s)),
        ],
    )
    .unwrap()
}

fn config() -> WriterConfig {
    WriterConfig {
        row_group_rows: 1 << 20,
        key_boundary_flush: false,
        write_batch_rows: 1024,
        data_page_bytes: 1 << 20,
        dictionary_page_bytes: 1 << 20,
        statistics: "page".into(),
        offset_index: true,
        compression: "zstd(1)".into(),
        columns: vec![],
    }
}

fn time_write(batches: &[RecordBatch]) -> f64 {
    let sorting = sorting_columns(&schema(), &["k".to_string()]);
    let props = config().writer_properties(sorting).unwrap();
    let mut sink: Vec<u8> = Vec::new();
    let (rows, timing) = measure::time(|| write_prepared(&mut sink, batches, schema(), props));
    rows.unwrap();
    timing.wall_seconds
}

#[test]
fn a_delay_outside_the_clock_does_not_change_the_measured_interval() {
    let batches = vec![batch(5_000)];
    // Simulate expensive validation/report writing happening after the timed region.
    let sorting = sorting_columns(&schema(), &["k".to_string()]);
    let props = config().writer_properties(sorting).unwrap();
    let mut sink: Vec<u8> = Vec::new();
    let (rows, timing) = measure::time(|| write_prepared(&mut sink, &batches, schema(), props));
    rows.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(60)); // "validation" after the clock
    // The recorded wall reflects only the write, which for 5k rows is well under 60ms.
    assert!(
        timing.wall_seconds < 0.05,
        "the post-clock delay leaked into the measured write: {}s",
        timing.wall_seconds
    );
}

#[test]
fn more_work_inside_the_clock_grows_the_measured_interval() {
    let small = time_write(&[batch(2_000)]);
    let large_batches: Vec<RecordBatch> = (0..40).map(|_| batch(2_000)).collect();
    let large = time_write(&large_batches);
    assert!(
        large > small,
        "writing 20x the rows should take longer: small={small}s large={large}s"
    );
}

#[test]
fn peak_rss_is_observed_on_this_platform() {
    let r = measure::rusage();
    assert!(
        r.max_rss_bytes.unwrap_or(0) > 0,
        "expected a peak RSS reading on Linux"
    );
}
