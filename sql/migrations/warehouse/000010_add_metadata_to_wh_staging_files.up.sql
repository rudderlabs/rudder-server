
--
-- wh_staging_Files
--

ALTER TABLE wh_staging_files ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';
