ALTER TABLE reports
ADD COLUMN IF NOT EXISTS source_batch_id text DEFAULT '',
ADD COLUMN IF NOT EXISTS source_task_id text DEFAULT '';
