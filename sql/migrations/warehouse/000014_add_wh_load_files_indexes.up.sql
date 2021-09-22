-- wh_load_files --

CREATE INDEX IF NOT EXISTS wh_load_files_uploadId_tableName_index ON wh_load_files (upload_id, table_name);

CREATE INDEX IF NOT EXISTS wh_load_files_srcId_destId_tableName_uploadId_index ON wh_load_files (source_id, destination_id, table_name, upload_id);

CREATE INDEX IF NOT EXISTS wh_load_files_uploadId_index ON wh_load_files (upload_id);
