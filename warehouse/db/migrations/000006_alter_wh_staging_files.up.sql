DO $$
BEGIN
  ALTER TABLE wh_staging_files
    ADD COLUMN IF NOT EXISTS first_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS total_events BIGINT;
END $$
