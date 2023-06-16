BEGIN;
INSERT INTO wh_staging_files (id, location, schema, source_id, destination_id, status, total_events, first_event_at,
                              last_event_at, created_at, updated_at, metadata)
VALUES (1, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:23:37.100685', NOW(), '{}'),
       (2, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:24:37.100685', NOW(), '{}'),
       (3, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:25:37.100685', NOW(), '{}'),
       (4, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:26:37.100685', NOW(), '{}'),
       (5, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:27:37.100685', NOW(), '{}'),
       (6, 'a.json.gz', '{}', 'test-sourceID', 'test-destinationID-1', 'succeeded', 1, NOW(), NOW(),
        '2022-12-06 15:27:37.100685', NOW(), '{}');
INSERT INTO wh_uploads(id, source_id, namespace, destination_id, destination_type, start_staging_file_id,
                       end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, first_event_at,
                       last_event_at, last_exec_at, timings, created_at, updated_at, metadata,
                       in_progress, workspace_id)
VALUES (1, 'test-sourceID', 'test-namespace', 'test-destinationID', 'POSTGRES', 0, 0, 0, 0, 'exported_data', '{}',
        '{}', NULL, NULL, NULL, NULL, '2022-12-06 15:30:00', '2022-12-06 15:45:00', '{}', TRUE,
        'test-workspaceID');
COMMIT;
