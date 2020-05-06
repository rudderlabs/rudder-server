DO $$
BEGIN
  ALTER TABLE wh_load_files ALTER COLUMN table_name TYPE TEXT;
  ALTER TABLE wh_load_files ADD COLUMN IF NOT EXISTS total_events BIGINT;
END $$
