CREATE TABLE IF NOT EXISTS partition_migration_dedup (
  key TEXT PRIMARY KEY,
  last_job_id BIGINT NOT NULL DEFAULT 0
);