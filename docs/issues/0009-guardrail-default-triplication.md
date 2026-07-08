# 0009 — Ingest guardrail defaults duplicated in three places

- **Severity:** Low (hygiene; drift risk, no runtime impact today)
- **Status:** Open — fix planned in roadmap row 28
- **Components:** `crates/ukiel-ingest`, `crates/ukield`
- **Found by:** post-landing review of plans 17/18, 2026-07-08 (`docs/notes/2026-07-08-plan17-18-review.md`)

## Summary

The ingest guardrail defaults (`l0_slowdown_parts = 30`, `l0_stop_parts =
200`, `warn_partitions_per_flush = 64`) live in three places:

1. `ukiel-ingest` `IngestConfig` serde `default =` fns,
2. `ukield` `IngestSection::Default`,
3. the ingest unit-test fixture `cfg()`.

Editing one silently diverges the others — the serde defaults apply when a
field is absent from a directly-deserialized `IngestConfig`, while ukield
users get `IngestSection`'s values.

(Not this issue: the compactor's library-vs-ukield default split — 2/2
test-compat vs 4/10 production — is intentional and commented.)

## Fix

Export the default fns (or a consts module) from `ukiel-ingest` and have
`IngestSection::Default` call them, making the crate the single source of
truth. The test fixture may keep explicit literals — its comment says the
band thresholds should read clearly in the assertions.
