DO $$
BEGIN

  BEGIN
    CREATE TYPE wh_upload_state_type
      AS ENUM(
              'waiting',
              'generating_load_file',
              'generating_load_file_failed',
              'generated_load_file',
              'updating_schema',
              'updating_schema_failed',
              'updated_schema',
              'exporting_data',
              'exporting_data_failed',
              'exported_data',
              'aborted'
      );
  EXCEPTION
    WHEN duplicate_object THEN null;
  END;

  CREATE TABLE IF NOT EXISTS wh_uploads (
    id BIGSERIAL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    namespace VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    destination_type VARCHAR(64) NOT NULL,
    start_staging_file_id BIGINT,
    end_staging_file_id BIGINT,
    start_load_file_id BIGINT,
    end_load_file_id BIGINT,
    status wh_upload_state_type NOT NULL,
    schema JSONB NOT NULL,
    error JSONB,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
  );

  -- index on status
  CREATE INDEX IF NOT EXISTS wh_uploads_status_index ON wh_uploads (status);

  -- index on source_id, destination_id combination
  CREATE INDEX IF NOT EXISTS wh_uploads_source_destination_id_index ON wh_uploads (source_id, destination_id);

END $$
