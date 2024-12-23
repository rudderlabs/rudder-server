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
    1, 1, 1, 1, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now()
  ),
  (
    2, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    2, 2, 2, 2, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now()
  ),
  (
    3, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    3, 3, 3, 3, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now()
  ),
  (
    4, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    4, 4, 4, 4, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now()
  );
INSERT INTO wh_staging_files (
  id, location, schema, source_id, destination_id,
  status, total_events, total_bytes, first_event_at,
  last_event_at, created_at, updated_at,
  metadata, upload_id
)
VALUES
  (
    1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, 231, NOW(), NOW(), NOW(),
    NOW(), '{}', 1
  ),
  (
    2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, 231, NOW(), NOW(), NOW(),
    NOW(), '{}', 2
  ),
  (
    3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, 231, NOW(), NOW(), NOW(),
    NOW(), '{}', 3
  ),
  (
    4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, 231, NOW(), NOW(), NOW(),
    NOW(), '{}', 4
  );
INSERT INTO wh_load_files (
  id, staging_file_id, location, source_id,
  destination_id, destination_type,
  table_name, total_events, created_at,
  metadata
)
VALUES
  (
    1, 1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW(),
    '{}'
  ),
  (
    2, 2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW(),
    '{}'
  ),
  (
    3, 3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW(),
    '{}'
  ),
  (
    4, 4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW(),
    '{}'
  );
COMMIT;
