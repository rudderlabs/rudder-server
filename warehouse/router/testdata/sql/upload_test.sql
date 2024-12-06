BEGIN;
INSERT INTO wh_uploads (
  id, source_id, namespace, destination_id,
  destination_type, start_staging_file_id,
  end_staging_file_id, start_load_file_id,
  end_load_file_id, status, schema,
  error, metadata, first_event_at,
  last_event_at, created_at, updated_at,
  timings
)
VALUES
  (
    1, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    1, 1, 1, 1, 'waiting', '{}', '{}', '{}',
    now(), now(), now(), now(), '[{ "exporting_data": "2020-04-21T15:16:19.687716Z", "exported_data": "2020-04-21T15:26:34.344356Z"}]'
  ),
  (
    2, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    2, 2, 2, 2, 'exporting_data_failed',
    '{}', '{}', '{}', now(), now(), now(),
    now(), '[]'
  ),
  (
    3, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    3, 3, 3, 3, 'aborted', '{}', '{}', '{}',
    now(), now(), now(), now(), '[]'
  ),
  (
    4, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    4, 4, 4, 4, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now(), '[]'
  ),
  (
    5, 'test-sourceID', 'test-namespace',
    'test-destinationID', 'POSTGRES',
    5, 5, 5, 5, 'exported_data', '{}', '{}',
    '{}', now(), now(), now(), now(), '[]'
  );
INSERT INTO wh_staging_files (
  id, location, schema, source_id, destination_id,
  status, total_events, first_event_at,
  last_event_at, created_at, updated_at,
  metadata,upload_id
)
VALUES
  (
    1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}',1
  ),
  (
    2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}',1
  ),
  (
    3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}',1
  ),
  (
    4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}',1
  ),
  (
    5, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}',1
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
  ),
  (
    5, 5, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW(),
    '{}'
  );
INSERT INTO wh_table_uploads (
  id, wh_upload_id, table_name, status,
  total_events, error, created_at,
  updated_at
)
VALUES
  (
    1, 1, 'test-table', 'waiting', 1, '',
    NOW(), NOW()
  ),
  (
    2, 2, 'test-table', 'exporting_data_failed',
    1, '', NOW(), NOW()
  ),
  (
    3, 3, 'test-table', 'aborted', 1, '',
    NOW(), NOW()
  ),
  (
    4, 4, 'test-table', 'exported_data',
    1, '', NOW(), NOW()
  ),
  (
    5, 5, 'test-table', 'exported_data',
    1, '', NOW(), NOW()
  );
COMMIT;
