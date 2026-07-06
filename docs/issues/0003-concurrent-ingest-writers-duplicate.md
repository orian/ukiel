# 0003 — Concurrent ingest workers double-commit (offset upsert is last-writer-wins)

- **Severity:** High (silent data duplication — but only under misconfiguration)
- **Status:** Resolved (plan 19) — the `ingest_offsets` upsert in `commit_inner` is now a CAS (`... DO UPDATE ... WHERE ingest_offsets.next_offset <= range.first`); a duplicate writer gets zero rows affected → `CatalogError::OffsetRace`, rolling the whole commit back (no duplicate part). `<=` keeps compacted-topic offset gaps legal. (Partition-subset leases remain future work.)
- **Components:** `crates/ukiel-catalog`, `crates/ukiel-ingest`
- **Found by:** design review, 2026-07-06

## Summary

Exactly-once ingest is currently exactly-once **per worker**, not per topic.
Two things compose badly:

1. `RouteIngest::connect` assigns **all** Kafka partitions of the topic to
   every worker (`consumer.rs` — explicit assignment, no partition subsets).
2. The `ingest_offsets` write in `commit_inner` (`commit.rs`) is an
   unconditional upsert: `ON CONFLICT ... DO UPDATE SET next_offset =
   EXCLUDED.next_offset` — last writer wins, no comparison with the stored
   position.

Run two `ukield` processes with the ingest role on the same topic (a copy-
paste deploy mistake, a rolling restart overlap, a second environment
pointed at the same catalog) and both consume every message and both commit
parts. Neither transaction conflicts — ADD commits don't conflict, and the
offset upsert happily moves the cursor backwards/forwards. Result: duplicate
rows in separate parts, indistinguishable from real data.

## Impact

Silent duplication; queries return inflated results. No error anywhere.
The design doc's "ingest scales with Kafka partitions" is currently
unimplemented *and* unguarded — the failure mode of trying it is corruption,
not an error message.

## Fix

Plan 19: **offset CAS**. The offsets write becomes conditional on the stored
position not having moved past the range being committed
(`WHERE ingest_offsets.next_offset <= $range.first` — `<=` not `=`, so
offset gaps from compacted topics stay legal). Zero rows updated → the whole
commit transaction rolls back with an explicit offset-race error and the
worker dies loudly. Misconfiguration becomes an error message instead of
duplicate data.

True horizontal ingest (partition-subset ownership, e.g. leases per
`(topic, kafka_partition)` in the catalog) stays future work — the CAS makes
it safe to build later by making the current single-writer assumption
enforced rather than assumed.

## Notes

- The crash/resume exactly-once tests (S2) don't catch this: they restart
  one worker, never run two concurrently.
- Poison-message offset advancement is unaffected — same single writer.
