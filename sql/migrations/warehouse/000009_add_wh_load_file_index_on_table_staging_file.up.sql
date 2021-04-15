-- wh_load_files --

CREATE INDEX IF NOT EXISTS wh_load_files_staging_file_id_table_name_index ON wh_load_files (staging_file_id, table_name);
