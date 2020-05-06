DO $$
BEGIN
  ALTER TABLE wh_load_files ALTER COLUMN table_name TYPE VARCHAR(64);
  ALTER TABLE wh_load_files DROP COLUMN IF EXISTS total_events;
END $$
