DO $$
BEGIN
  ALTER TABLE wh_table_uploads ALTER COLUMN error TYPE VARCHAR(64);
  ALTER TABLE wh_table_uploads DROP COLUMN IF EXISTS total_events;
END $$
