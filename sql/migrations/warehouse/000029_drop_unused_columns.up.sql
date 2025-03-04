ALTER TABLE wh_schemas
    DROP COLUMN IF EXISTS wh_upload_id,
    DROP COLUMN IF EXISTS error;

ALTER TABLE wh_uploads DROP COLUMN IF EXISTS mergedschema;
