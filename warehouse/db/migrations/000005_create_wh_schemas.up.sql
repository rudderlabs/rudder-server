DO $$
BEGIN

  CREATE TABLE IF NOT EXISTS wh_schemas (
									  id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  created_at TIMESTAMP NOT NULL);

  -- index on source_id, destination_id combination
  CREATE INDEX IF NOT EXISTS wh_schemas_source_destination_id_index ON wh_schemas (source_id, destination_id);

END $$
