-- wh_uploads --

CREATE INDEX IF NOT EXISTS wh_uploads_id_destination_id_namespace_status_index ON wh_uploads (id, destination_id, namespace, status);

CREATE INDEX IF NOT EXISTS wh_uploads_destination_id_namespace_index ON wh_uploads (destination_id, namespace);

CREATE INDEX IF NOT EXISTS wh_uploads_destination_type_status_destination_id_namespace_concatenated_index ON wh_uploads (destination_type, status, (destination_id || '_' || namespace))
