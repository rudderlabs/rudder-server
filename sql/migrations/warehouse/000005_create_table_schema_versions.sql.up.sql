--
-- wh_schema_versions
--

CREATE TABLE IF NOT EXISTS wh_schema_versions (
                                          id BIGSERIAL PRIMARY KEY,
                                          wh_schema_id BIGSERIAL NOT NULL,
                                          schema JSONB NOT NULL,
                                          created_at TIMESTAMP NOT NULL);
CREATE INDEX IF NOT EXISTS wh_schema_versions_wh_schema_id_index ON wh_schema_versions (wh_schema_id);
