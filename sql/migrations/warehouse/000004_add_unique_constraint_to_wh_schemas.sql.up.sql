--
-- wh_schemas
--

ALTER TABLE wh_schemas ADD CONSTRAINT unique_wh_identifier UNIQUE("source_id","destination_id","namespace")
