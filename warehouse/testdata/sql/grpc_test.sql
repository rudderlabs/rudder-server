BEGIN;
INSERT INTO wh_uploads (
  id, source_id, namespace, destination_id,
  destination_type, start_staging_file_id,
  end_staging_file_id, start_load_file_id,
  end_load_file_id, status, schema,
  error, metadata, first_event_at,
  last_event_at, created_at, updated_at
)
VALUES
  (
    1, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    1, 1, 1, 1, 'waiting', '{}', '{}', '{}',
    now(), now(), now(), now()
  ),
  (
    2, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    1, 1, 1, 1, 'exporting_data_failed',
    '{}', '{}', '{}', now(), now(), now(),
    now()
  ),
  (
    3, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    1, 1, 1, 1, 'aborted', '{}', '{}', '{}',
    now(), now(), now(), now()
  ),
  (
    4, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    1, 1, 1, 1, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now()
  );
INSERT INTO wh_table_uploads (
  id, wh_upload_id, table_name, status,
  error, created_at, updated_at
)
VALUES
  (
    1, 1, 'test-table', 'waiting', '', NOW(),
    NOW()
  ),
  (
    2, 2, 'test-table', 'failed', '', NOW(),
    NOW()
  ),
  (
    3, 3, 'test-table', 'failed', '', NOW(),
    NOW()
  ),
  (
    4, 4, 'test-table', 'succeeded', '',
    NOW(), NOW()
  );
COMMIT;
