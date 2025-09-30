-- Add index on wh_staging_files table to speed up queries on source_id, destination_id, upload_id, and id columns (e.g. for cron tracker query)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wh_staging_files_sid_did_upload_id_id_asc
ON wh_staging_files (source_id, destination_id, upload_id, id ASC);
