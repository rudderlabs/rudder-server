BEGIN;
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
    5, 1, 'rudder/rudder-warehouse-staging-logs/2EUralUySYUs7hgsdU1lFXRSm/2022-09-20/1663650685.2EUralsdsDyZjOKU1lFXRSm.eeadsb4-a066-42f4-a90b-460161378e1b.json.gz',
    'test-sourceID', 'test-destinationID',
    'POSTGRES', 'rudder-discards', 1,
    NOW(), '{}'
  );
END;
