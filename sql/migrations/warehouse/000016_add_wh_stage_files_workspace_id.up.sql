
--
-- wh_staging_files
--

ALTER TABLE wh_staging_files ADD COLUMN IF NOT EXISTS workspace_id VARCHAR NOT NULL DEFAULT '';
