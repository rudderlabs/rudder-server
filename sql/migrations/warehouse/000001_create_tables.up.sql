
--
-- wh_staging_files
--

CREATE TABLE IF NOT EXISTS wh_staging_files (
    id BIGSERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    schema JSONB NOT NULL,
    error TEXT,
    status VARCHAR(64),
    first_event_at TIMESTAMP,
    last_event_at TIMESTAMP,
    total_events BIGINT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL);

ALTER TABLE wh_staging_files
    ADD COLUMN IF NOT EXISTS first_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS total_events BIGINT;

CREATE INDEX IF NOT EXISTS wh_staging_files_id_index ON wh_staging_files (source_id, destination_id);

ALTER TABLE wh_staging_files ALTER COLUMN status TYPE VARCHAR(64);

DO $$ BEGIN
    DROP TYPE wh_staging_state_type;
    EXCEPTION WHEN OTHERS THEN null;
END $$;

--
-- wh_load_files
--

CREATE TABLE IF NOT EXISTS wh_load_files (
    id BIGSERIAL PRIMARY KEY,
    staging_file_id BIGINT,
    location TEXT NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    destination_type VARCHAR(64) NOT NULL,
    table_name TEXT NOT NULL,
    total_events BIGINT,
    created_at TIMESTAMP NOT NULL);

ALTER TABLE wh_load_files ALTER COLUMN table_name TYPE TEXT;

ALTER TABLE wh_load_files ADD COLUMN IF NOT EXISTS total_events BIGINT;

CREATE INDEX IF NOT EXISTS wh_load_files_source_destination_id_table_name_index ON wh_load_files (source_id, destination_id, table_name);

--
-- wh_uploads
--

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
    status VARCHAR(64) NOT NULL,
    schema JSONB NOT NULL,
    error JSONB,
    first_event_at TIMESTAMP,
    last_event_at TIMESTAMP,
    last_exec_at TIMESTAMP,
    timings JSONB,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL);

ALTER TABLE wh_uploads
    ADD COLUMN IF NOT EXISTS first_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_exec_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS timings JSONB;

CREATE INDEX IF NOT EXISTS wh_uploads_status_index ON wh_uploads (status);

CREATE INDEX IF NOT EXISTS wh_uploads_source_destination_id_index ON wh_uploads (source_id, destination_id);

ALTER TABLE wh_uploads ALTER COLUMN status TYPE VARCHAR(64);

DO $$ BEGIN
    DROP TYPE wh_upload_state_type;
    EXCEPTION WHEN OTHERS THEN null;
END $$;

--
-- wh_table_uploads
--

CREATE TABLE IF NOT EXISTS 	wh_table_uploads (
    id BIGSERIAL PRIMARY KEY,
    wh_upload_id BIGSERIAL NOT NULL,
    table_name TEXT,
    status VARCHAR(64) NOT NULL,
    error TEXT,
    last_exec_time TIMESTAMP,
    total_events TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL);

ALTER TABLE wh_table_uploads ADD COLUMN IF NOT EXISTS total_events BIGINT;

ALTER TABLE wh_table_uploads ALTER COLUMN error TYPE TEXT;

CREATE INDEX IF NOT EXISTS wh_table_uploads_wh_upload_id_table_name_index ON wh_table_uploads (wh_upload_id, table_name);

ALTER TABLE wh_table_uploads ALTER COLUMN status TYPE VARCHAR(64);

DO $$ BEGIN
    DROP TYPE wh_table_upload_state_type;
    EXCEPTION WHEN OTHERS THEN null;
END $$;

--
-- wh_schemas
--

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

DROP INDEX IF EXISTS wh_schemas_source_destination_id_index;

CREATE INDEX IF NOT EXISTS wh_schemas_destination_id_namespace_index ON wh_schemas (destination_id, namespace);