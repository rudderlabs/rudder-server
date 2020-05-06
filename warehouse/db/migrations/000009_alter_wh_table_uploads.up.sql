DO $$
BEGIN
  ALTER TABLE wh_table_uploads ALTER COLUMN error TYPE TEXT;
  ALTER TABLE wh_table_uploads ADD COLUMN IF NOT EXISTS total_events BIGINT;
END $$
