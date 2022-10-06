
--
-- wh_stage_files
--

ALTER TABLE wh_stage_files ADD COLUMN IF NOT EXISTS workspace_id VARCHAR NOT NULL DEFAULT '';
