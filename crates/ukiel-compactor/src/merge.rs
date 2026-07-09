//! K-way streaming merge over already-sorted part streams (plan 29).
//!
//! Memory is O(K · batch + one output batch): each of the K cursors holds one
//! decoded input batch and its sort-key `Rows`; the heap holds K owned rows.
//! The merge **never re-sorts** — plan 27's validated on-disk ordering is the
//! trust anchor — so an input that violates its order is a loud
//! `MergeOrderViolation` naming the part, never a silent fallback to an
//! O(partition) sort (the very thing that used to mask writer-ordering bugs).

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::SortOptions;
use arrow::compute::interleave_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use futures::Stream;
use futures::stream::StreamExt;
use ukiel_core::sorting::{SORT_DESCENDING, SORT_NULLS_FIRST};

use crate::CompactorError;
use crate::rewrite::{MERGE_BATCH_ROWS, PartBatchStream};

/// The canonical sort options (plan 27: ascending, nulls first), so the merge
/// comparator agrees with the on-disk ordering it trusts.
fn canonical_options() -> SortOptions {
    SortOptions {
        descending: SORT_DESCENDING,
        nulls_first: SORT_NULLS_FIRST,
    }
}

struct Cursor {
    stream: futures::stream::BoxStream<'static, Result<RecordBatch, CompactorError>>,
    path: String,
    /// Current decoded batch. Kept even after exhaustion so it stays a valid
    /// entry for `interleave_record_batch` (indices never reference it then).
    batch: RecordBatch,
    /// Sort-key `Rows` for `batch`.
    rows: Rows,
    /// Next row to emit within `batch`.
    idx: usize,
    exhausted: bool,
    /// Last row of the batches consumed so far (the cross-batch order guard).
    last: Option<OwnedRow>,
}

/// Converts a batch's sort columns to `Rows` and verifies order: monotonic
/// within the batch and non-regressing versus the stream's previous batch.
fn convert_and_guard(
    converter: &RowConverter,
    sort_indices: &[usize],
    batch: &RecordBatch,
    path: &str,
    prev_last: Option<&OwnedRow>,
) -> Result<(Rows, OwnedRow), CompactorError> {
    let cols: Vec<ArrayRef> = sort_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();
    let rows = converter.convert_columns(&cols)?;
    for i in 1..rows.num_rows() {
        if rows.row(i) < rows.row(i - 1) {
            return Err(CompactorError::MergeOrderViolation {
                path: path.to_string(),
                row: i,
            });
        }
    }
    if let Some(prev) = prev_last
        && rows.row(0).owned() < *prev
    {
        return Err(CompactorError::MergeOrderViolation {
            path: path.to_string(),
            row: 0,
        });
    }
    let last = rows.row(rows.num_rows() - 1).owned();
    Ok((rows, last))
}

struct MergeState {
    cursors: Vec<Cursor>,
    /// Min-heap (via `Reverse`) of `(current row, stream idx)`.
    heap: BinaryHeap<Reverse<(OwnedRow, usize)>>,
    converter: RowConverter,
    sort_indices: Vec<usize>,
}

impl MergeState {
    async fn init(
        streams: Vec<PartBatchStream>,
        schema: SchemaRef,
        sort_key: &[String],
    ) -> Result<Self, CompactorError> {
        let sort_indices: Vec<usize> = sort_key
            .iter()
            .map(|n| {
                schema
                    .index_of(n)
                    .map_err(|_| CompactorError::MissingColumn(n.clone()))
            })
            .collect::<Result<_, _>>()?;
        let fields = sort_indices
            .iter()
            .map(|&i| {
                SortField::new_with_options(
                    schema.field(i).data_type().clone(),
                    canonical_options(),
                )
            })
            .collect();
        let converter = RowConverter::new(fields)?;

        // Placeholder empty batch per cursor until `refill` pulls the first
        // real one (Rows isn't Clone, so build one per cursor).
        let empty = RecordBatch::new_empty(schema.clone());
        let empty_cols: Vec<ArrayRef> = sort_indices
            .iter()
            .map(|&i| empty.column(i).clone())
            .collect();
        let mut cursors = Vec::with_capacity(streams.len());
        for ps in streams {
            cursors.push(Cursor {
                stream: ps.stream,
                path: ps.path,
                batch: empty.clone(),
                rows: converter.convert_columns(&empty_cols)?,
                idx: 0,
                exhausted: false,
                last: None,
            });
        }

        let mut state = MergeState {
            cursors,
            heap: BinaryHeap::new(),
            converter,
            sort_indices,
        };
        for s in 0..state.cursors.len() {
            state.refill(s).await?;
        }
        Ok(state)
    }

    /// Pulls the next non-empty batch for cursor `s` (guarding its order) and
    /// pushes its head to the heap; marks the cursor exhausted at end-of-stream.
    async fn refill(&mut self, s: usize) -> Result<(), CompactorError> {
        loop {
            match self.cursors[s].stream.next().await {
                None => {
                    self.cursors[s].exhausted = true;
                    return Ok(());
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                Some(Ok(batch)) => {
                    let (rows, last) = convert_and_guard(
                        &self.converter,
                        &self.sort_indices,
                        &batch,
                        &self.cursors[s].path,
                        self.cursors[s].last.as_ref(),
                    )?;
                    let head = rows.row(0).owned();
                    let c = &mut self.cursors[s];
                    c.batch = batch;
                    c.rows = rows;
                    c.idx = 0;
                    c.last = Some(last);
                    self.heap.push(Reverse((head, s)));
                    return Ok(());
                }
            }
        }
    }

    fn build(&self, indices: &[(usize, usize)]) -> Result<RecordBatch, CompactorError> {
        let batches: Vec<&RecordBatch> = self.cursors.iter().map(|c| &c.batch).collect();
        Ok(interleave_record_batch(&batches, indices)?)
    }

    /// Produces the next merged batch (≤ `MERGE_BATCH_ROWS` rows), or `None`
    /// when every stream is drained.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, CompactorError> {
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(MERGE_BATCH_ROWS);
        loop {
            let Some(Reverse((_row, s))) = self.heap.pop() else {
                return if indices.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(self.build(&indices)?))
                };
            };
            indices.push((s, self.cursors[s].idx));
            self.cursors[s].idx += 1;

            if self.cursors[s].idx >= self.cursors[s].batch.num_rows() {
                // Batch exhausted: flush (indices still reference live batches),
                // then refill — order matters, `build` reads the old batch.
                let out = self.build(&indices)?;
                self.refill(s).await?;
                return Ok(Some(out));
            }

            // Push the cursor's new head.
            let idx = self.cursors[s].idx;
            let head = self.cursors[s].rows.row(idx).owned();
            self.heap.push(Reverse((head, s)));

            if indices.len() >= MERGE_BATCH_ROWS {
                return Ok(Some(self.build(&indices)?));
            }
        }
    }
}

enum MaybeState {
    Init {
        streams: Vec<PartBatchStream>,
        schema: SchemaRef,
        sort_key: Vec<String>,
    },
    Running(MergeState),
}

/// Merges K already-sorted part streams into one sorted batch stream (batches
/// of ≤ `MERGE_BATCH_ROWS`). Comparison is via `arrow::row::RowConverter` under
/// the canonical plan-27 options, so the loser heap is type-agnostic. Never
/// sorts: an out-of-order input is a loud `MergeOrderViolation`.
pub fn merge_streams(
    streams: Vec<PartBatchStream>,
    schema: SchemaRef,
    sort_key: &[String],
) -> impl Stream<Item = Result<RecordBatch, CompactorError>> {
    let sort_key = sort_key.to_vec();
    futures::stream::try_unfold(
        MaybeState::Init {
            streams,
            schema,
            sort_key,
        },
        |state| async move {
            let mut state = match state {
                MaybeState::Init {
                    streams,
                    schema,
                    sort_key,
                } => MergeState::init(streams, schema, &sort_key).await?,
                MaybeState::Running(s) => s,
            };
            match state.next_batch().await? {
                Some(batch) => Ok(Some((batch, MaybeState::Running(state)))),
                None => Ok(None),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn stream_of(path: &str, batches: Vec<RecordBatch>) -> PartBatchStream {
        PartBatchStream {
            path: path.to_string(),
            stream: futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
        }
    }

    fn rows_of(batches: &[RecordBatch]) -> Vec<(i64, i64, String)> {
        let mut out = Vec::new();
        for b in batches {
            let t: &Int64Array = b.column(0).as_any().downcast_ref().unwrap();
            let ts: &Int64Array = b.column(1).as_any().downcast_ref().unwrap();
            let p: &StringArray = b.column(2).as_any().downcast_ref().unwrap();
            for i in 0..b.num_rows() {
                out.push((t.value(i), ts.value(i), p.value(i).to_string()));
            }
        }
        out
    }

    async fn merge_all(
        streams: Vec<PartBatchStream>,
        sort_key: &[String],
    ) -> Result<Vec<RecordBatch>, CompactorError> {
        let s = merge_streams(streams, schema(), sort_key);
        futures::pin_mut!(s);
        let mut out = Vec::new();
        while let Some(b) = s.next().await {
            out.push(b?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn merges_sorted_multiset_preserving_across_shapes() {
        let sk = vec!["tenant_id".to_string(), "ts".to_string()];
        // Uneven lengths, duplicate keys across streams, a key spanning a
        // batch boundary within stream A (1,10)|(1,20), an empty stream.
        let a = stream_of(
            "a",
            vec![
                batch(&[(1, 10, "a1")]),
                batch(&[(1, 20, "a2"), (3, 5, "c"), (4, 1, "d")]),
            ],
        );
        let b = stream_of(
            "b",
            vec![batch(&[(1, 15, "b1"), (2, 5, "b2"), (3, 5, "c2")])],
        );
        let empty = stream_of("empty", vec![]);
        let out = merge_all(vec![a, b, empty], &sk).await.unwrap();

        assert!(out.iter().all(|b| b.num_rows() <= MERGE_BATCH_ROWS));
        let got = rows_of(&out);
        assert!(
            got.windows(2).all(|w| (w[0].0, w[0].1) <= (w[1].0, w[1].1)),
            "output sorted by (tenant_id, ts): {got:?}"
        );
        let mut got_sorted = got.clone();
        got_sorted.sort();
        let mut expect: Vec<(i64, i64, String)> = [
            (1, 10, "a1"),
            (1, 20, "a2"),
            (3, 5, "c"),
            (4, 1, "d"),
            (1, 15, "b1"),
            (2, 5, "b2"),
            (3, 5, "c2"),
        ]
        .iter()
        .map(|(a, b, c)| (*a, *b, c.to_string()))
        .collect();
        expect.sort();
        assert_eq!(got_sorted, expect, "no rows lost or duplicated");
    }

    #[tokio::test]
    async fn single_stream_passes_through_sorted() {
        let sk = vec!["tenant_id".to_string(), "ts".to_string()];
        let a = stream_of("a", vec![batch(&[(1, 1, "x"), (1, 2, "y"), (2, 1, "z")])]);
        let out = merge_all(vec![a], &sk).await.unwrap();
        assert_eq!(
            rows_of(&out),
            vec![(1, 1, "x".into()), (1, 2, "y".into()), (2, 1, "z".into())]
        );
    }

    #[tokio::test]
    async fn nullable_middle_column_sorts_nulls_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
        ]));
        let mk = |region: Option<&str>, ts: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1_i64])),
                    Arc::new(StringArray::from(vec![region])),
                    Arc::new(Int64Array::from(vec![ts])),
                ],
            )
            .unwrap()
        };
        let sk = vec![
            "tenant_id".to_string(),
            "region".to_string(),
            "ts".to_string(),
        ];
        // Stream A: null region; Stream B: "a" region. Null must sort first.
        let a = PartBatchStream {
            path: "a".into(),
            stream: futures::stream::iter(vec![Ok(mk(None, 5))]).boxed(),
        };
        let b = PartBatchStream {
            path: "b".into(),
            stream: futures::stream::iter(vec![Ok(mk(Some("a"), 5))]).boxed(),
        };
        let s = merge_streams(vec![a, b], schema.clone(), &sk);
        futures::pin_mut!(s);
        let mut regions = Vec::new();
        while let Some(batch) = s.next().await {
            let batch = batch.unwrap();
            let r: &StringArray = batch.column(1).as_any().downcast_ref().unwrap();
            for i in 0..batch.num_rows() {
                regions.push(r.is_valid(i).then(|| r.value(i).to_string()));
            }
        }
        assert_eq!(regions, vec![None, Some("a".to_string())], "nulls first");
    }

    #[tokio::test]
    async fn out_of_order_input_is_a_loud_error_naming_the_part() {
        let sk = vec!["tenant_id".to_string(), "ts".to_string()];
        // Within-batch: (2,..) before (1,..).
        let bad = stream_of("bad.parquet", vec![batch(&[(2, 5, "x"), (1, 5, "y")])]);
        match merge_all(vec![bad], &sk).await {
            Err(CompactorError::MergeOrderViolation { path, .. }) => {
                assert_eq!(path, "bad.parquet")
            }
            other => panic!("expected MergeOrderViolation, got {other:?}"),
        }
        // Cross-batch: batch 2 starts before batch 1 ended.
        let bad2 = stream_of(
            "bad2.parquet",
            vec![batch(&[(1, 10, "a")]), batch(&[(1, 5, "b")])],
        );
        match merge_all(vec![bad2], &sk).await {
            Err(CompactorError::MergeOrderViolation { path, row }) => {
                assert_eq!(path, "bad2.parquet");
                assert_eq!(row, 0, "cross-batch regression flagged at row 0");
            }
            other => panic!("expected MergeOrderViolation, got {other:?}"),
        }
    }
}
