-- Issue 0013: a fencing token must never be reused.
--
-- Plan 41 made `generation` a per-row counter: expiry takeover bumped it, but a
-- clean release *deletes* the row, so the next acquisition inserted a fresh row
-- at generation 1. That permits
--
--     (partition, owner A, generation 1) -> release -> (partition, owner A, generation 1)
--
-- and (partition, owner, generation) is exactly what renew, release and the
-- fenced REPLACE compare. A token the caller happened to retain across the
-- release — a delayed cleanup task, in-process compaction concurrency, or a
-- reconciliation lifecycle holding an operation across a rebuild (plan 43 is
-- precisely that) — could then renew, release or commit against a *later*
-- tenancy of the same process.
--
-- A catalog-owned sequence makes the token globally non-reused instead of
-- row-locally monotonic. Every new tenancy (fresh row or takeover of an
-- expired/released one) draws `nextval`; an idempotent reacquisition by the
-- same unexpired owner keeps its generation, because an in-flight merge's fence
-- must survive its own retry.
--
-- Gaps are expected and harmless: a losing contender's `INSERT ... ON CONFLICT`
-- still evaluates the VALUES list, so it burns a value it never uses. The token
-- needs uniqueness and ordering, not density.
CREATE SEQUENCE compaction_lease_generation_seq AS BIGINT;

-- An upgrade, not just a fresh install: start strictly above every generation
-- already handed out, or a live tenancy could be re-issued as a "new" one.
-- `is_called => false` means the next `nextval` returns exactly this value.
SELECT setval(
    'compaction_lease_generation_seq',
    coalesce((SELECT max(generation) FROM compaction_leases), 0) + 1,
    false
);
