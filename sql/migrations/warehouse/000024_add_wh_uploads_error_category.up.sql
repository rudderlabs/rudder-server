--
-- wh_uploads
--

ALTER TABLE wh_uploads ADD COLUMN IF NOT EXISTS error_category VARCHAR NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS wh_uploads_error_category_index ON wh_uploads(error_category);
