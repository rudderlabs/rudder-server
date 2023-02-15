
--
-- wh_async_jobs
--

ALTER TABLE wh_async_jobs ADD COLUMN IF NOT EXISTS workspace_id VARCHAR NOT NULL DEFAULT '';
