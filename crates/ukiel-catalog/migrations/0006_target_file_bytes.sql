-- Placement generalized to one knob (spec: size-targeted placement):
-- NULL = packed (one file per merge output),
-- 0 = separated (every packing key its own files),
-- N > 0 = size-targeted (merge outputs cut at key boundaries into ~N-byte
-- files; keys bigger than N get dedicated files -> heavy keys separate
-- organically).
ALTER TABLE hypertables
    ADD COLUMN target_file_bytes BIGINT
    CHECK (target_file_bytes IS NULL OR target_file_bytes >= 0);

UPDATE hypertables SET target_file_bytes = 0 WHERE placement = 'separated';

ALTER TABLE hypertables DROP COLUMN placement;
