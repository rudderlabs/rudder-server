
--
-- wh_staging_files
--

ALTER TABLE wh_staging_files ADD COLUMN IF NOT EXISTS upload_id INTEGER REFERENCES wh_uploads (id) ON DELETE SET NULL(upload_id);

CREATE INDEX wh_staging_files_upload_id_index ON wh_staging_files(upload_id);
