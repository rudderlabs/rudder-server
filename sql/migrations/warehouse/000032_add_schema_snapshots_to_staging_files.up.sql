-- Create a table to store schema snapshots for staging files.
-- Each snapshot represents the schema at a specific point in time for a (source, destination, workspace).
CREATE TABLE IF NOT EXISTS wh_staging_file_schema_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique identifier for the snapshot
    schema TEXT NOT NULL,                          -- JSON-encoded schema at the time of the snapshot
    source_id VARCHAR(64) NOT NULL,                -- Source identifier
    destination_id VARCHAR(64) NOT NULL,           -- Destination identifier
    workspace_id VARCHAR(64) NOT NULL,             -- Workspace identifier
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW() -- Snapshot creation timestamp
);

-- Add columns to wh_staging_files to link each file to a schema snapshot and store a JSON Patch diff.
ALTER TABLE wh_staging_files
    ADD COLUMN IF NOT EXISTS schema_snapshot_id UUID REFERENCES wh_staging_file_schema_snapshots(id) ON DELETE SET NULL DEFAULT NULL,   -- Link to the schema snapshot
    ADD COLUMN IF NOT EXISTS schema_snapshot_patch TEXT DEFAULT NULL;    -- JSON Patch (RFC 6902) diff from the snapshot to the actual schema

-- For fast lookup of staging files by schema_snapshot_id for archiving purposes.
CREATE INDEX IF NOT EXISTS wh_staging_files_schema_snapshot_id_index
    ON wh_staging_files(schema_snapshot_id);

-- For efficient retrieval of latest snapshot by (source_id, destination_id, created_at DESC).
CREATE INDEX IF NOT EXISTS wh_staging_file_schema_snapshots_source_id_destination_id_created_at_index
    ON wh_staging_file_schema_snapshots(source_id, destination_id, created_at DESC);

-- For filtering snapshots by workspace_id for migration purposes.
CREATE INDEX IF NOT EXISTS wh_staging_file_schema_snapshots_workspace_id_index
    ON wh_staging_file_schema_snapshots(workspace_id);
