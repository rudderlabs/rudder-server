CREATE TABLE IF NOT EXISTS buffered_partitions_versions (
  key TEXT PRIMARY KEY,
  version BIGINT NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION bump_buffered_partitions_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  lock_name text := TG_ARGV[0];
  tblname text := 'bumped_v_' || gen_random_uuid(); -- generate a unique temp table name
  already_bumped boolean; -- to avoid multiple version bumps in the same transaction
BEGIN
  -- create a temp table to track if we've already bumped the version in this transaction
  EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS %I (key text PRIMARY KEY) ON COMMIT DROP', tblname);
  -- check if we've already bumped the version in this transaction
  EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE key = %L)', tblname, lock_name) INTO already_bumped;
  IF NOT already_bumped THEN
  	PERFORM 1 FROM buffered_partitions_versions WHERE key = lock_name FOR UPDATE; -- lock the row
  	UPDATE buffered_partitions_versions SET version = version + 1 WHERE key = lock_name; -- bump version
    EXECUTE format('INSERT INTO %I(key) VALUES (%L)', tblname, lock_name); -- mark as bumped
  END IF;
  RETURN NULL;
END;
$$;