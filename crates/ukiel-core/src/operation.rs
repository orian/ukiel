//! Canonical logical identities for retryable catalog mutations (plan 43).
//!
//! When the connection dies around `COMMIT`, the caller does not know whether
//! its work landed. Plans 41/42 survive that — they rebuild from offsets and
//! live parts — but they cannot *answer* it. An identity makes the outcome
//! decidable: the catalog stores it atomically with the commit, so a later
//! lookup says `Committed(id)` or `Absent`.
//!
//! The identity names **logical intent**, not one physical attempt. Two workers
//! merging the same input parts to the same level are the same operation even
//! though they generate different object paths, different upload UUIDs, and hold
//! different leases. Fingerprinting an attempt instead of an intent would make
//! every retry a new operation and the whole mechanism a no-op — so generated
//! paths, UUIDs, lease owner/generation, attempt counters, process ids and
//! wall-clock time are all deliberately *absent* from the canonical bytes.
//!
//! Canonical bytes are type-tagged and length-delimited. Nothing here uses
//! `Debug`, unordered map iteration, or `serde` enum encoding: those are
//! representations that a refactor is allowed to change, and a fingerprint whose
//! meaning changes silently is worse than no fingerprint at all. When a code or
//! policy change *does* alter the logical transformation, the caller bumps its
//! transformation version, which is inside the fingerprint.

use serde_json::Value;

use crate::ids::{HypertableId, PartId};
use crate::table::Placement;

/// Bound on the stored key. The key carries a digest, not the payload, so it is
/// independent of how many parts or offset ranges an operation covers.
pub const MAX_OPERATION_KEY_LEN: usize = 128;

/// The identity of one logical catalog mutation.
///
/// The key is derived from the fingerprint by the factories below, but the
/// catalog stores and compares *both*. That is not redundancy: the boundary must
/// still catch a buggy caller, a legacy row, or direct SQL that pairs a key with
/// different intent. A key collision is a loud permanent error, never a quiet
/// "already applied".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationIdentity {
    hypertable_id: HypertableId,
    key: String,
    fingerprint: [u8; 32],
}

impl OperationIdentity {
    pub fn hypertable_id(&self) -> HypertableId {
        self.hypertable_id
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }

    /// The domain and version prefix, for structured logs. Bounded, and safe to
    /// log — unlike the key, which is a low-cardinality *label* nowhere.
    pub fn domain(&self) -> &str {
        self.key
            .split('/')
            .nth(1)
            .expect("a factory-built key always has a domain segment")
    }
}

/// One consumed Kafka offset range. Mirrors the catalog's `OffsetRange`, which
/// lives a layer up; the identity is built from the *intent* to consume these
/// exact offsets, before a single Parquet byte is written.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IngestRange {
    pub topic: String,
    pub partition: i32,
    /// First consumed offset, inclusive.
    pub first: i64,
    /// Last consumed offset, inclusive.
    pub last: i64,
}

/// Everything that makes one compaction merge the transformation it is. Output
/// `PartMeta` is *not* here: paths, sizes and statistics are properties of the
/// attempt, and two attempts at the same intent legitimately differ in all three.
#[derive(Debug, Clone, Copy)]
pub struct CompactionIntent<'a> {
    pub output_level: i16,
    pub input_parts: &'a [PartId],
    pub partition_values: &'a Value,
    pub table_schema: &'a Value,
    pub sort_key: &'a [String],
    pub placement: Placement,
    /// Bumped when the same inputs would legitimately produce different output.
    pub transformation_version: u32,
}

/// The intent behind one key deletion: which key, out of which live parts, under
/// which table shape.
#[derive(Debug, Clone, Copy)]
pub struct DeleteKeyIntent<'a> {
    pub packing_key: i64,
    pub input_parts: &'a [PartId],
    pub table_schema: &'a Value,
    pub sort_key: &'a [String],
    pub placement: Placement,
    pub transformation_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperationError {
    #[error("operation has no input parts")]
    NoInputParts,
    #[error("duplicate input part {0} in one operation")]
    DuplicateInputPart(i64),
    #[error("operation has no offset ranges")]
    NoOffsetRanges,
    #[error("offset range has an empty topic")]
    EmptyTopic,
    #[error("offset range {topic}/{partition} is inverted: {first}..={last}")]
    InvertedOffsetRange {
        topic: String,
        partition: i32,
        first: i64,
        last: i64,
    },
    #[error("offset ranges overlap on {topic}/{partition}: {first}..={last}")]
    OverlappingOffsetRanges {
        topic: String,
        partition: i32,
        first: i64,
        last: i64,
    },
    #[error("operation has an empty sort key")]
    EmptySortKey,
    #[error("operation key is {0} bytes, over the {MAX_OPERATION_KEY_LEN}-byte bound")]
    KeyTooLong(usize),
}

// Canonical type tags. Never reuse a value for a different type: the tag is what
// keeps `"1"` from colliding with `1`, and an empty list from colliding with an
// empty string. `0x01` is retired (an opaque-bytes tag no identity input needed)
// — a new value type takes a new tag, never a recycled one, or the pinned
// vectors below would silently change meaning.
const TAG_STR: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_LIST: u8 = 0x04;
const TAG_JSON_NULL: u8 = 0x10;
const TAG_JSON_BOOL: u8 = 0x11;
const TAG_JSON_NUMBER: u8 = 0x12;
const TAG_JSON_STRING: u8 = 0x13;
const TAG_JSON_ARRAY: u8 = 0x14;
const TAG_JSON_OBJECT: u8 = 0x15;

/// A type-tagged, length-delimited byte string. Every value is self-describing,
/// so no two different intents can encode to the same bytes by accident.
#[derive(Default)]
struct Canonical(Vec<u8>);

impl Canonical {
    fn len(&mut self, n: usize) {
        self.0.extend_from_slice(&(n as u64).to_be_bytes());
    }

    fn str(&mut self, s: &str) -> &mut Self {
        self.0.push(TAG_STR);
        self.len(s.len());
        self.0.extend_from_slice(s.as_bytes());
        self
    }

    fn int(&mut self, v: i64) -> &mut Self {
        self.0.push(TAG_INT);
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// An *ordered* list: the caller decides whether the order is semantic (a
    /// sort key) or normalized (sorted part ids). The encoder never reorders.
    fn list<T>(&mut self, items: &[T], mut each: impl FnMut(&mut Self, &T)) -> &mut Self {
        self.0.push(TAG_LIST);
        self.len(items.len());
        for item in items {
            each(self, item);
        }
        self
    }

    /// Canonical JSON: object keys sorted, array order preserved, scalar types
    /// and JSON-null distinguished. A partition value or table schema that only
    /// differs in key order is the same intent.
    fn json(&mut self, v: &Value) -> &mut Self {
        match v {
            Value::Null => self.0.push(TAG_JSON_NULL),
            Value::Bool(b) => {
                self.0.push(TAG_JSON_BOOL);
                self.0.push(u8::from(*b));
            }
            Value::Number(n) => {
                self.0.push(TAG_JSON_NUMBER);
                let s = n.to_string();
                self.len(s.len());
                self.0.extend_from_slice(s.as_bytes());
            }
            Value::String(s) => {
                self.0.push(TAG_JSON_STRING);
                self.len(s.len());
                self.0.extend_from_slice(s.as_bytes());
            }
            Value::Array(items) => {
                self.0.push(TAG_JSON_ARRAY);
                self.len(items.len());
                for item in items {
                    self.json(item);
                }
            }
            Value::Object(map) => {
                self.0.push(TAG_JSON_OBJECT);
                self.len(map.len());
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for k in keys {
                    self.len(k.len());
                    self.0.extend_from_slice(k.as_bytes());
                    self.json(&map[k]);
                }
            }
        }
        self
    }

    fn placement(&mut self, p: Placement) -> &mut Self {
        // Tagged by variant, not by `to_db()`: the database encoding is a
        // storage detail, and Separated's `Some(0)` sharing a type with
        // SizeTargeted's `Some(n)` is exactly the kind of collapse a
        // fingerprint must not inherit.
        match p {
            Placement::Packed => self.str("packed"),
            Placement::Separated => self.str("separated"),
            Placement::SizeTargeted(n) => self.str("size_targeted").int(n),
        }
    }
}

/// Sorted, de-duplicated part ids. Two workers enumerating the same live group
/// in different orders must produce the same identity; the *same* part twice is
/// a malformed intent, not a normalization problem.
fn canonical_parts(parts: &[PartId]) -> Result<Vec<i64>, OperationError> {
    if parts.is_empty() {
        return Err(OperationError::NoInputParts);
    }
    let mut ids: Vec<i64> = parts.iter().map(|p| p.0).collect();
    ids.sort_unstable();
    if let Some(dup) = ids.windows(2).find(|w| w[0] == w[1]) {
        return Err(OperationError::DuplicateInputPart(dup[0]));
    }
    Ok(ids)
}

/// Sorted ranges, with overlap rejected per `(topic, partition)`. Overlapping
/// ranges are not two spellings of one intent — they are an ingest bug, and
/// hashing them would hide it behind a stable-looking key.
fn canonical_ranges(ranges: &[IngestRange]) -> Result<Vec<IngestRange>, OperationError> {
    if ranges.is_empty() {
        return Err(OperationError::NoOffsetRanges);
    }
    let mut sorted = ranges.to_vec();
    sorted.sort();
    for r in &sorted {
        if r.topic.is_empty() {
            return Err(OperationError::EmptyTopic);
        }
        if r.first > r.last {
            return Err(OperationError::InvertedOffsetRange {
                topic: r.topic.clone(),
                partition: r.partition,
                first: r.first,
                last: r.last,
            });
        }
    }
    for w in sorted.windows(2) {
        let (prev, next) = (&w[0], &w[1]);
        if prev.topic == next.topic && prev.partition == next.partition && next.first <= prev.last {
            return Err(OperationError::OverlappingOffsetRanges {
                topic: next.topic.clone(),
                partition: next.partition,
                first: next.first,
                last: next.last,
            });
        }
    }
    Ok(sorted)
}

/// Hashes the canonical bytes and formats the stored key. The domain and version
/// are *inside* the hashed bytes as well as in the key text: the prefix is for
/// humans and for collision-avoidance against historical keys, the hashed copy
/// is what makes the domains cryptographically separate.
fn finish(
    hypertable_id: HypertableId,
    domain: &'static str,
    version: u32,
    body: impl FnOnce(&mut Canonical),
) -> Result<OperationIdentity, OperationError> {
    let mut canon = Canonical::default();
    canon.str("ukiel");
    canon.str(domain);
    canon.int(i64::from(version));
    canon.int(hypertable_id.0);
    body(&mut canon);

    let fingerprint: [u8; 32] = blake3::hash(&canon.0).into();
    let key = format!("ukiel/{domain}/v{version}/{}", hex(&fingerprint));
    if key.len() > MAX_OPERATION_KEY_LEN {
        return Err(OperationError::KeyTooLong(key.len()));
    }
    Ok(OperationIdentity {
        hypertable_id,
        key,
        fingerprint,
    })
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

impl OperationIdentity {
    /// The intent to consume exactly these Kafka offsets into this hypertable.
    ///
    /// Deliberately *not* a function of the rows, the Parquet bytes, or the
    /// output paths: a retry of the same flush reads the same offsets and must
    /// reconcile to the same operation even though it produces different files.
    pub fn ingest(
        hypertable_id: HypertableId,
        ranges: &[IngestRange],
        transformation_version: u32,
    ) -> Result<Self, OperationError> {
        let ranges = canonical_ranges(ranges)?;
        finish(hypertable_id, "ingest", 1, |c| {
            c.int(i64::from(transformation_version));
            c.list(&ranges, |c, r| {
                c.str(&r.topic).int(i64::from(r.partition));
                c.int(r.first).int(r.last);
            });
        })
    }

    /// The intent to merge exactly these live parts into one run at
    /// `output_level`, under this table's shape.
    pub fn compaction(
        hypertable_id: HypertableId,
        intent: CompactionIntent<'_>,
    ) -> Result<Self, OperationError> {
        let parts = canonical_parts(intent.input_parts)?;
        if intent.sort_key.is_empty() {
            return Err(OperationError::EmptySortKey);
        }
        finish(hypertable_id, "compaction", 1, |c| {
            c.int(i64::from(intent.transformation_version));
            c.int(i64::from(intent.output_level));
            c.list(&parts, |c, id| {
                c.int(*id);
            });
            c.json(intent.partition_values);
            c.json(intent.table_schema);
            c.list(intent.sort_key, |c, k| {
                c.str(k);
            });
            c.placement(intent.placement);
        })
    }

    /// The intent to remove one packing key from exactly these live parts.
    pub fn delete_key(
        hypertable_id: HypertableId,
        intent: DeleteKeyIntent<'_>,
    ) -> Result<Self, OperationError> {
        let parts = canonical_parts(intent.input_parts)?;
        if intent.sort_key.is_empty() {
            return Err(OperationError::EmptySortKey);
        }
        finish(hypertable_id, "delete-key", 1, |c| {
            c.int(i64::from(intent.transformation_version));
            c.int(intent.packing_key);
            c.list(&parts, |c, id| {
                c.int(*id);
            });
            c.json(intent.table_schema);
            c.list(intent.sort_key, |c, k| {
                c.str(k);
            });
            c.placement(intent.placement);
        })
    }

    /// Rehydrates an identity the catalog is about to look up. Only for callers
    /// that already hold a factory-built key/fingerprint pair (the supervisor
    /// carries one across a worker rebuild); it cannot mint new intent.
    pub fn from_stored(
        hypertable_id: HypertableId,
        key: String,
        fingerprint: [u8; 32],
    ) -> Result<Self, OperationError> {
        if key.len() > MAX_OPERATION_KEY_LEN {
            return Err(OperationError::KeyTooLong(key.len()));
        }
        Ok(Self {
            hypertable_id,
            key,
            fingerprint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parts(ids: &[i64]) -> Vec<PartId> {
        ids.iter().copied().map(PartId).collect()
    }

    fn intent<'a>(
        input: &'a [PartId],
        partition: &'a Value,
        schema: &'a Value,
        sort_key: &'a [String],
    ) -> CompactionIntent<'a> {
        CompactionIntent {
            output_level: 1,
            input_parts: input,
            partition_values: partition,
            table_schema: schema,
            sort_key,
            placement: Placement::Packed,
            transformation_version: 1,
        }
    }

    fn sort_key() -> Vec<String> {
        vec!["tenant_id".into(), "ts".into()]
    }

    /// The vectors that matter: what must *not* change the identity.
    #[test]
    fn identity_is_invariant_under_input_order_and_json_key_order() {
        let ht = HypertableId(7);
        let sk = sort_key();
        let a = OperationIdentity::compaction(
            ht,
            intent(
                &parts(&[3, 1, 2]),
                &json!({"day": "2026-07-01", "region": "eu"}),
                &json!({"fields": [{"name": "ts", "type": "timestamp_ms"}]}),
                &sk,
            ),
        )
        .unwrap();
        let b = OperationIdentity::compaction(
            ht,
            intent(
                &parts(&[2, 3, 1]),
                &json!({"region": "eu", "day": "2026-07-01"}),
                &json!({"fields": [{"type": "timestamp_ms", "name": "ts"}]}),
                &sk,
            ),
        )
        .unwrap();
        assert_eq!(a, b, "enumeration order and JSON key order are not intent");
    }

    /// ...and what must.
    #[test]
    fn every_transformation_input_changes_the_identity() {
        let ht = HypertableId(7);
        let sk = sort_key();
        let partition = json!({"day": "2026-07-01"});
        let schema = json!({"fields": [{"name": "ts", "type": "timestamp_ms"}]});
        let two = parts(&[1, 2]);
        let three = parts(&[1, 2, 3]);
        let base =
            OperationIdentity::compaction(ht, intent(&two, &partition, &schema, &sk)).unwrap();

        let mut different = Vec::new();
        // A different hypertable is a different operation even byte-for-byte.
        different.push(
            OperationIdentity::compaction(HypertableId(8), intent(&two, &partition, &schema, &sk))
                .unwrap(),
        );
        // Input set.
        different.push(
            OperationIdentity::compaction(ht, intent(&three, &partition, &schema, &sk)).unwrap(),
        );
        // Output level.
        let mut level = intent(&two, &partition, &schema, &sk);
        level.output_level = 2;
        different.push(OperationIdentity::compaction(ht, level).unwrap());
        // Partition.
        different.push(
            OperationIdentity::compaction(
                ht,
                intent(&two, &json!({"day": "2026-07-02"}), &schema, &sk),
            )
            .unwrap(),
        );
        // Schema.
        different.push(
            OperationIdentity::compaction(
                ht,
                intent(
                    &two,
                    &partition,
                    &json!({"fields": [{"name": "ts", "type": "int64"}]}),
                    &sk,
                ),
            )
            .unwrap(),
        );
        // Sort key *order* is semantic, unlike part order.
        let reversed = vec!["ts".to_string(), "tenant_id".to_string()];
        different.push(
            OperationIdentity::compaction(ht, intent(&two, &partition, &schema, &reversed))
                .unwrap(),
        );
        // Placement, including the two variants the database encoding collapses
        // toward each other.
        for placement in [
            Placement::Separated,
            Placement::SizeTargeted(64),
            Placement::SizeTargeted(128),
        ] {
            let mut p = intent(&two, &partition, &schema, &sk);
            p.placement = placement;
            different.push(OperationIdentity::compaction(ht, p).unwrap());
        }
        // Transformation version: the caller's escape hatch when the same inputs
        // would now legitimately produce different output.
        let mut version = intent(&two, &partition, &schema, &sk);
        version.transformation_version = 2;
        different.push(OperationIdentity::compaction(ht, version).unwrap());

        for other in &different {
            assert_ne!(&base, other, "collided with the base intent");
        }
        let unique: std::collections::HashSet<_> = different.iter().map(|i| i.key()).collect();
        assert_eq!(unique.len(), different.len(), "two variants collided");
    }

    #[test]
    fn domains_are_separated() {
        let ht = HypertableId(1);
        let sk = sort_key();
        let schema = json!({"fields": []});
        let compaction = OperationIdentity::compaction(
            ht,
            CompactionIntent {
                output_level: 1,
                input_parts: &parts(&[1]),
                partition_values: &json!({}),
                table_schema: &schema,
                sort_key: &sk,
                placement: Placement::Packed,
                transformation_version: 1,
            },
        )
        .unwrap();
        let deletion = OperationIdentity::delete_key(
            ht,
            DeleteKeyIntent {
                packing_key: 1,
                input_parts: &parts(&[1]),
                table_schema: &schema,
                sort_key: &sk,
                placement: Placement::Packed,
                transformation_version: 1,
            },
        )
        .unwrap();
        assert_ne!(compaction.fingerprint(), deletion.fingerprint());
        assert_eq!(compaction.domain(), "compaction");
        assert_eq!(deletion.domain(), "delete-key");
    }

    #[test]
    fn ingest_identity_is_the_offset_intent() {
        let ht = HypertableId(3);
        let a = OperationIdentity::ingest(
            ht,
            &[
                IngestRange {
                    topic: "events".into(),
                    partition: 1,
                    first: 10,
                    last: 19,
                },
                IngestRange {
                    topic: "events".into(),
                    partition: 0,
                    first: 0,
                    last: 9,
                },
            ],
            1,
        )
        .unwrap();
        let b = OperationIdentity::ingest(
            ht,
            &[
                IngestRange {
                    topic: "events".into(),
                    partition: 0,
                    first: 0,
                    last: 9,
                },
                IngestRange {
                    topic: "events".into(),
                    partition: 1,
                    first: 10,
                    last: 19,
                },
            ],
            1,
        )
        .unwrap();
        assert_eq!(a, b, "range enumeration order is not intent");

        // Each field is load-bearing.
        for other in [
            OperationIdentity::ingest(
                ht,
                &[IngestRange {
                    topic: "events".into(),
                    partition: 0,
                    first: 0,
                    last: 9,
                }],
                1,
            )
            .unwrap(),
            OperationIdentity::ingest(
                ht,
                &[
                    IngestRange {
                        topic: "other".into(),
                        partition: 0,
                        first: 0,
                        last: 9,
                    },
                    IngestRange {
                        topic: "events".into(),
                        partition: 1,
                        first: 10,
                        last: 19,
                    },
                ],
                1,
            )
            .unwrap(),
            OperationIdentity::ingest(
                ht,
                &[
                    IngestRange {
                        topic: "events".into(),
                        partition: 0,
                        first: 0,
                        last: 10,
                    },
                    IngestRange {
                        topic: "events".into(),
                        partition: 1,
                        first: 11,
                        last: 19,
                    },
                ],
                1,
            )
            .unwrap(),
            OperationIdentity::ingest(
                ht,
                &[
                    IngestRange {
                        topic: "events".into(),
                        partition: 0,
                        first: 0,
                        last: 9,
                    },
                    IngestRange {
                        topic: "events".into(),
                        partition: 1,
                        first: 10,
                        last: 19,
                    },
                ],
                2,
            )
            .unwrap(),
        ] {
            assert_ne!(a, other);
        }
    }

    #[test]
    fn malformed_intent_is_rejected_before_any_catalog_io() {
        let ht = HypertableId(1);
        let sk = sort_key();
        let schema = json!({});
        let partition = json!({});

        assert_eq!(
            OperationIdentity::compaction(ht, intent(&[], &partition, &schema, &sk)).unwrap_err(),
            OperationError::NoInputParts
        );
        assert_eq!(
            OperationIdentity::compaction(ht, intent(&parts(&[1, 1]), &partition, &schema, &sk))
                .unwrap_err(),
            OperationError::DuplicateInputPart(1)
        );
        assert_eq!(
            OperationIdentity::ingest(ht, &[], 1).unwrap_err(),
            OperationError::NoOffsetRanges
        );
        assert_eq!(
            OperationIdentity::ingest(
                ht,
                &[IngestRange {
                    topic: String::new(),
                    partition: 0,
                    first: 0,
                    last: 1
                }],
                1
            )
            .unwrap_err(),
            OperationError::EmptyTopic
        );
        assert!(matches!(
            OperationIdentity::ingest(
                ht,
                &[IngestRange {
                    topic: "t".into(),
                    partition: 0,
                    first: 5,
                    last: 4
                }],
                1
            )
            .unwrap_err(),
            OperationError::InvertedOffsetRange { .. }
        ));
        // Overlapping ranges are an ingest bug, not two spellings of one intent.
        assert!(matches!(
            OperationIdentity::ingest(
                ht,
                &[
                    IngestRange {
                        topic: "t".into(),
                        partition: 0,
                        first: 0,
                        last: 9
                    },
                    IngestRange {
                        topic: "t".into(),
                        partition: 0,
                        first: 9,
                        last: 12
                    },
                ],
                1
            )
            .unwrap_err(),
            OperationError::OverlappingOffsetRanges { .. }
        ));
        // Adjacent-but-disjoint ranges on one partition are legal.
        OperationIdentity::ingest(
            ht,
            &[
                IngestRange {
                    topic: "t".into(),
                    partition: 0,
                    first: 0,
                    last: 9,
                },
                IngestRange {
                    topic: "t".into(),
                    partition: 0,
                    first: 10,
                    last: 12,
                },
            ],
            1,
        )
        .unwrap();
        // Same offsets on different partitions never overlap.
        OperationIdentity::ingest(
            ht,
            &[
                IngestRange {
                    topic: "t".into(),
                    partition: 0,
                    first: 0,
                    last: 9,
                },
                IngestRange {
                    topic: "t".into(),
                    partition: 1,
                    first: 0,
                    last: 9,
                },
            ],
            1,
        )
        .unwrap();
    }

    /// The key is a digest, so its length does not track the operation's size —
    /// which is what lets an arbitrarily large merge have a bounded catalog key.
    #[test]
    fn key_length_is_bounded_and_independent_of_input_size() {
        let ht = HypertableId(1);
        let sk = sort_key();
        let schema = json!({});
        let partition = json!({});
        let one = parts(&[1]);
        let small =
            OperationIdentity::compaction(ht, intent(&one, &partition, &schema, &sk)).unwrap();
        let huge: Vec<PartId> = (1..=10_000).map(PartId).collect();
        let big =
            OperationIdentity::compaction(ht, intent(&huge, &partition, &schema, &sk)).unwrap();
        assert_eq!(small.key().len(), big.key().len());
        assert!(big.key().len() <= MAX_OPERATION_KEY_LEN);
        assert_ne!(small, big);

        assert_eq!(
            OperationIdentity::from_stored(ht, "x".repeat(MAX_OPERATION_KEY_LEN + 1), [0; 32])
                .unwrap_err(),
            OperationError::KeyTooLong(MAX_OPERATION_KEY_LEN + 1)
        );
    }

    /// Pinned vectors. An accidental encoder change — a reordered field, a
    /// dropped tag, a different hash — must fail here and become a deliberate
    /// versioning decision, not a silent reinterpretation of history.
    #[test]
    fn canonical_vectors_are_pinned() {
        let sk = sort_key();
        let compaction = OperationIdentity::compaction(
            HypertableId(7),
            CompactionIntent {
                output_level: 1,
                input_parts: &parts(&[2, 1]),
                partition_values: &json!({"day": "2026-07-01"}),
                table_schema: &json!({"fields": [{"name": "ts", "type": "timestamp_ms"}]}),
                sort_key: &sk,
                placement: Placement::Packed,
                transformation_version: 1,
            },
        )
        .unwrap();
        assert_eq!(
            compaction.key(),
            "ukiel/compaction/v1/cb70ac8c5bd33e9783019b895e154ceb02939933e3fbc503e5496478890a81af"
        );

        let ingest = OperationIdentity::ingest(
            HypertableId(3),
            &[IngestRange {
                topic: "events".into(),
                partition: 0,
                first: 0,
                last: 9,
            }],
            1,
        )
        .unwrap();
        assert_eq!(
            ingest.key(),
            "ukiel/ingest/v1/72d2778de52e88b219117e1b17c4afcdaaf640d591d700e00500b4f753ca0d80"
        );
    }
}
