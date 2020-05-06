DO $$
BEGIN

  BEGIN
    CREATE TYPE wh_staging_state_type
      AS ENUM(
              'waiting',
              'executing',
              'failed',
              'succeeded'
      );
  EXCEPTION
    WHEN duplicate_object THEN null;
  END;

  CREATE TABLE IF NOT EXISTS wh_staging_files (
    id BIGSERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    schema JSONB NOT NULL,
    error TEXT,
    status wh_staging_state_type,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
  );

  -- index on source_id, destination_id combination
  CREATE INDEX IF NOT EXISTS wh_staging_files_id_index ON wh_staging_files (source_id, destination_id);

END $$
