
--
-- wh_uploads
--

ALTER TABLE wh_uploads ADD COLUMN IF NOT EXISTS mergedschema JSONB DEFAULT '{}';
