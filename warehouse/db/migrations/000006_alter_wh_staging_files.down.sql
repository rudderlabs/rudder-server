DO $$
BEGIN
  ALTER TABLE wh_staging_files
    DROP COLUMN IF EXISTS first_event_at,
    DROP COLUMN IF EXISTS last_event_at,
    DROP COLUMN IF EXISTS total_events;
END $$
