--
-- wh_schemas
--

ALTER TABLE wh_schemas
    ADD COLUMN IF NOT EXISTS schema_compressed bytea;
