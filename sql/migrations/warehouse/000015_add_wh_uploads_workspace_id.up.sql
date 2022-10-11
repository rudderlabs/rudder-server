
--
-- wh_uploads
--

ALTER TABLE wh_uploads ADD COLUMN IF NOT EXISTS workspace_id VARCHAR NOT NULL DEFAULT '';
