DO $$
BEGIN
  DROP TABLE IF EXISTS wh_uploads;
  DROP INDEX IF EXISTS wh_uploads_status_index;
  DROP INDEX IF EXISTS wh_uploads_source_destination_id_index;
  DROP TYPE IF EXISTS wh_upload_state_type;
END $$
