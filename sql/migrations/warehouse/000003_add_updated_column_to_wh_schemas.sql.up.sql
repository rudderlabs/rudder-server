--
-- wh_schemas
--

ALTER TABLE wh_schemas ADD COLUMN IF NOT EXISTS updated_at timestamp;
