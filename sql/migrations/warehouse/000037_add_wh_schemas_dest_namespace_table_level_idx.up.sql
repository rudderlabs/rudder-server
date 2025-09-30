-- Add index on wh_schemas table for table-level schema query
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_schemas_dest_namespace_table_level_idx
ON wh_schemas (destination_id, namespace, table_name, source_id DESC)
WHERE table_name != '';
