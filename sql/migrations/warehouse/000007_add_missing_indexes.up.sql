
-- wh_load_files --

CREATE INDEX IF NOT EXISTS wh_load_files_staging_file_id_index ON wh_load_files (staging_file_id);

CREATE INDEX IF NOT EXISTS wh_load_files_id_source_id_destination_id_table_name_index ON wh_load_files (id, source_id, destination_id, table_name);


-- wh_staging_files --

CREATE INDEX IF NOT EXISTS wh_staging_files_id_source_id_destination_id_index ON wh_staging_files (id, source_id, destination_id);


-- wh_table_uploads --

CREATE INDEX IF NOT EXISTS wh_table_uploads_wh_upload_id_index ON wh_table_uploads (wh_upload_id);

CREATE INDEX IF NOT EXISTS wh_table_uploads_wh_upload_id_status_index ON wh_table_uploads (wh_upload_id, status);


-- wh_uploads --

CREATE INDEX IF NOT EXISTS wh_uploads_id_destination_id_namespace_status_index ON wh_uploads (id, destination_id, namespace, status);

CREATE INDEX IF NOT EXISTS wh_uploads_destination_type_destination_id_source_id_status_index ON wh_uploads (destination_type, destination_id, source_id, status);

CREATE INDEX IF NOT EXISTS wh_uploads_destination_id_namespace_index ON wh_uploads (destination_id, namespace);

CREATE INDEX IF NOT EXISTS wh_uploads_created_at_index ON wh_uploads (created_at);
