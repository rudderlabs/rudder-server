-- Add index on wh_schemas table to speed up queries on destination_id, namespace, and table_name columns (e.g. for schema snapshot query)
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_schemas_dest_namespace_empty_table_idx
    ON wh_schemas (destination_id, namespace, table_name) 
    WHERE table_name = '';
