ALTER TABLE reports
DROP COLUMN IF EXISTS source_batch_id,
DROP COLUMN IF EXISTS source_task_id;
