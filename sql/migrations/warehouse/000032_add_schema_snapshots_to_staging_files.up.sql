-- Add schema snapshots table for staging files
CREATE TABLE IF NOT EXISTS wh_staging_file_schema_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    source_id VARCHAR(64) NOT NULL,
    destination_id VARCHAR(64) NOT NULL,
    workspace_id VARCHAR(64) NOT NULL
);

-- Add columns to wh_staging_files to link to snapshots and store diffs
ALTER TABLE wh_staging_files
    ADD COLUMN IF NOT EXISTS schema_snapshot_id UUID REFERENCES wh_staging_file_schema_snapshots(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS schema_diff TEXT; -- Store JSON Patch diff

-- Index for fast lookup by snapshot
CREATE INDEX IF NOT EXISTS wh_staging_files_schema_snapshot_id_index ON wh_staging_files(schema_snapshot_id);

CREATE INDEX IF NOT EXISTS wh_staging_file_schema_snapshots_source_id_destination_id_index ON wh_staging_file_schema_snapshots(source_id, destination_id);
CREATE INDEX IF NOT EXISTS wh_staging_file_schema_snapshots_workspace_id_index ON wh_staging_file_schema_snapshots(workspace_id); 