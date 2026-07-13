-- Plan 43: the fingerprint of the logical operation a commit applied.
--
-- `idempotency_key` already exists and already carries the operation key, so it
-- keeps its name and its unique index. What it cannot do alone is *prove* that a
-- key means what the caller thinks it means. Two things could pair one key with
-- different intent: a buggy or legacy caller, and direct SQL. The stored digest
-- is what turns that from a silent "already applied" — the worst possible
-- answer, since it would tell a worker that someone else's commit was its own —
-- into a loud permanent error.
--
-- Nullable, because historical keyed commits exist and their intent is *not*
-- recoverable. Backfilling a made-up digest would convert unknown history into
-- false certainty, so a NULL fingerprint under a looked-up key fails loudly
-- instead. New application writes set key and fingerprint together or neither;
-- the new keys are `ukiel/<domain>/v<n>/<hex>`, so a collision with an old key
-- is practically unreachable anyway.
ALTER TABLE commits
    ADD COLUMN operation_fingerprint BYTEA
        CHECK (operation_fingerprint IS NULL OR octet_length(operation_fingerprint) = 32);
