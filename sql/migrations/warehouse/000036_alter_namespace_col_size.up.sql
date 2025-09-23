-- Increase the size of the namespace column in the wh_schemas and wh_uploads tables to 128 characters
ALTER TABLE wh_schemas ALTER COLUMN namespace TYPE VARCHAR(128);
ALTER TABLE wh_uploads ALTER COLUMN namespace TYPE VARCHAR(128);
