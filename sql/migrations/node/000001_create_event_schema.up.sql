---
--- Event Models
---

CREATE TABLE IF NOT EXISTS event_models (
		id SERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL,
		write_key VARCHAR(32) NOT NULL,
		event_type TEXT NOT NULL,
		event_model_identifier TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMP NOT NULL DEFAULT NOW());

CREATE INDEX IF NOT EXISTS event_model_write_key_index ON event_models (write_key);

---
---  Schema Versions
---

CREATE TABLE IF NOT EXISTS schema_versions (
		id BIGSERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL, 
		event_model_id VARCHAR(36) NOT NULL,
		schema_hash VARCHAR(32) NOT NULL,
		schema JSONB NOT NULL,
		metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
		first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
		last_seen TIMESTAMP NOT NULL DEFAULT NOW());

CREATE INDEX IF NOT EXISTS event_model_id_index ON schema_versions (event_model_id);
CREATE UNIQUE INDEX IF NOT EXISTS event_model_id_schema_hash_index ON schema_versions (event_model_id, schema_hash);