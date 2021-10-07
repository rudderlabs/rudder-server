
--
-- wh_uploads
--

CREATE INDEX IF NOT EXISTS wh_uploads_destination_type_in_progress_status_destination_id_namespace_concatenated_index ON wh_uploads (destination_type, in_progress, status, (destination_id || '_' || namespace));

DROP INDEX IF EXISTS wh_uploads_destination_type_status_destination_id_namespace_concatenated_index;
