DO $$
BEGIN

  CREATE TABLE IF NOT EXISTS wh_load_files (
    id BIGSERIAL PRIMARY KEY,
    staging_file_id BIGINT,
    location TEXT NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    destination_type VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL
  );

	-- index on source_id, destination_id, table_name combination
  CREATE INDEX IF NOT EXISTS wh_load_files_source_destination_id_table_name_index ON wh_load_files (source_id, destination_id, table_name);

END $$
