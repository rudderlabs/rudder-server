-- Add index on wh_staging_files table to speed up queries on source_id, destination_id, and created_at columns (e.g. for cron tracker query)
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_staging_files_source_id_destination_id_created_at_idx ON wh_staging_files(source_id, destination_id, created_at);
