BEGIN;
INSERT INTO wh_staging_files (
  id, location, schema, source_id, destination_id,
  status, total_events, first_event_at,
  last_event_at, created_at, updated_at,
  metadata
)
VALUES
  (
    1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}'
  ),
  (
    2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}'
  ),
  (
    3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}'
  ),
  (
    4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    '{}', 'test-sourceID', 'test-destinationID',
    'succeeded', 1, NOW(), NOW(), NOW(),
    NOW(), '{}'
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
    'POSTGRES', 'test-table', 1, NOW() + INTERVAL '1 seconds',
    '{}'
  ),
  (
    2, 2, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW()+ INTERVAL '2 seconds',
    '{}'
  ),
  (
    3, 3, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW()+ INTERVAL '3 seconds',
    '{}'
  ),
  (
    4, 4, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'test-table', 1, NOW()+ INTERVAL '4 seconds',
    '{}'
  );
INSERT INTO wh_table_uploads (
  id, wh_upload_id, table_name, status,
  total_events, error, created_at,
  updated_at
)
VALUES
  (
    1, 1, 'test-table-1', 'exported_data',
    1, '', NOW(), NOW()
  ),
  (
    2, 1, 'test-table-2', 'exported_data',
    1, '', NOW(), NOW()
  ),
  (
    3, 1, 'test-table-3', 'exported_data',
    1, '', NOW(), NOW()
  ),
  (
    4, 1, 'test-table-4', 'exported_data',
    1, '', NOW(), NOW()
  );
COMMIT;
