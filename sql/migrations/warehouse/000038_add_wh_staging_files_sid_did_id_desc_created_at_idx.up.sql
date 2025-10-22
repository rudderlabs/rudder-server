-- Add index on wh_staging_files table for cron tracker query
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_staging_files_sid_did_id_desc_created_at_idx
ON wh_staging_files (
    source_id,
    destination_id,
    id DESC,
    created_at
);
