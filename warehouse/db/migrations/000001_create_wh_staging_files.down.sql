DO $$
BEGIN
  DROP TABLE IF EXISTS wh_staging_files;
  DROP INDEX IF EXISTS wh_staging_files_id_index;
  DROP TYPE IF EXISTS wh_staging_state_type;
END $$
