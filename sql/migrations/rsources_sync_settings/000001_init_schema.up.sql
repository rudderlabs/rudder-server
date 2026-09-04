CREATE TABLE IF NOT EXISTS rsources_sync_settings (
    job_run_id TEXT PRIMARY KEY,
    store_error_responses BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
