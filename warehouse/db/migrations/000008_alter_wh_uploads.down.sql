DO $$
BEGIN
  ALTER TABLE wh_uploads
    DROP COLUMN IF EXISTS first_event_at,
    DROP COLUMN IF EXISTS last_event_at,
    DROP COLUMN IF EXISTS last_exec_at;
    DROP COLUMN IF EXISTS timings;
END $$
