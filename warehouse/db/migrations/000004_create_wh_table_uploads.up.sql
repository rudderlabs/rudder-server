DO $$
BEGIN

  BEGIN
    CREATE TYPE wh_table_upload_state_type
      AS ENUM(
              'waiting',
              'executing',
              'exporting_data',
              'exporting_data_failed',
              'exported_data',
              'aborted'
      );
  EXCEPTION
    WHEN duplicate_object THEN null;
  END;

  CREATE TABLE IF NOT EXISTS wh_table_uploads (
    id BIGSERIAL PRIMARY KEY,
    wh_upload_id BIGSERIAL NOT NULL,
    table_name TEXT,
    status wh_table_upload_state_type NOT NULL,
    error VARCHAR(64),
    last_exec_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
  );

	-- index on source_id, destination_id combination
  CREATE INDEX IF NOT EXISTS wh_table_uploads_wh_upload_id_table_name_index ON wh_table_uploads (wh_upload_id, table_name);

END $$
