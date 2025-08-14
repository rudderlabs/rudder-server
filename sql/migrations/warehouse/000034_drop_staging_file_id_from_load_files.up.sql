-- Drop staging_file_id column from wh_load_files table
ALTER TABLE wh_load_files DROP COLUMN IF EXISTS staging_file_id; 