-- Drop the existing unique constraint
ALTER TABLE wh_schemas DROP CONSTRAINT IF EXISTS unique_wh_identifier;

-- Add the table_name column to wh_schemas
ALTER TABLE wh_schemas ADD COLUMN IF NOT EXISTS table_name TEXT DEFAULT '';

-- Add the unique constraint
ALTER TABLE wh_schemas ADD CONSTRAINT unique_wh_identifier UNIQUE("source_id","destination_id","namespace", "table_name");
