BEGIN;
INSERT INTO wh_schemas(id, source_id, namespace, destination_id, destination_type,
                        schema, error, created_at, updated_at)
VALUES (1, 'test-sourceID', 'test-namespace', 'test-destinationID', 'POSTGRES','{}', NULL,
        '2022-12-06 15:23:37.100685', '2022-12-06 15:23:37.100685');
COMMIT;
