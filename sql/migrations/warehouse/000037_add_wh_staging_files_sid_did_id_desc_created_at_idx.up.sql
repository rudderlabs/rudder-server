-- Add index on wh_staging_files table to speed up queries on source_id, destination_id, id, and created_at columns (e.g. for cron tracker query)
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_staging_files_sid_did_id_desc_created_at_idx
ON wh_staging_files (
    source_id,
    destination_id,
    id DESC,
    created_at
);
