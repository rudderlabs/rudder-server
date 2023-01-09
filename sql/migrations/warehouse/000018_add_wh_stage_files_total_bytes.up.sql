--
-- wh_staging_files
--

ALTER TABLE wh_staging_files ADD COLUMN IF NOT EXISTS total_bytes VARCHAR NOT NULL DEFAULT '';
