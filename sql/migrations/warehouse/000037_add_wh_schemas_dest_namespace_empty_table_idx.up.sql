-- Add index on wh_schemas table to speed up queries on database level schemas on destination_id, namespace where table_name is empty
CREATE INDEX CONCURRENTLY IF NOT EXISTS wh_schemas_dest_namespace_empty_table_idx
    ON wh_schemas (destination_id, namespace, table_name) 
    WHERE table_name = '';
