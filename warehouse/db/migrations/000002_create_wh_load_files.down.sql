DO $$
BEGIN
  DROP TABLE IF EXISTS wh_load_files;
  DROP INDEX IF EXISTS wh_load_files_source_destination_id_table_name_index;
END $$
