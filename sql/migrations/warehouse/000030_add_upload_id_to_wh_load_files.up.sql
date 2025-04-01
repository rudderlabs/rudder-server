ALTER TABLE wh_load_files ADD COLUMN IF NOT EXISTS upload_id INTEGER REFERENCES wh_uploads (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS wh_load_files_upload_id_index ON wh_load_files (upload_id); 