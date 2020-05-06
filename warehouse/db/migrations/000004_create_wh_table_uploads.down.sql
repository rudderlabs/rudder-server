DO $$
BEGIN
  DROP TABLE IF EXISTS wh_table_uploads;
  DROP INDEX IF EXISTS wh_table_uploads_wh_upload_id_table_name_index;
  DROP TYPE IF EXISTS wh_table_upload_state_type;
END $$
