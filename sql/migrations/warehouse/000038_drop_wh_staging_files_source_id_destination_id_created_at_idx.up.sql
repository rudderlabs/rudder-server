-- Drop the old index as it's superseded by the new composite index in migration 000037
DROP INDEX CONCURRENTLY IF EXISTS wh_staging_files_source_id_destination_id_created_at_idx;
