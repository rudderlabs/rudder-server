
--
-- wh_uploads
--

ALTER TABLE wh_uploads ADD COLUMN IF NOT EXISTS in_progress BOOL NOT NULL DEFAULT false;
