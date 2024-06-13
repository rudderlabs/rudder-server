CREATE TABLE tracked_users_reports (
                           id BIGSERIAL PRIMARY KEY,
                           workspace_id VARCHAR(64) NOT NULL,
                           instance_id VARCHAR(64) NOT NULL,
                           source_id VARCHAR(64),
                           reported_at TIMESTAMPTZ NOT NULL,
                           userid_hll TEXT NOT NULL,
                           anonymousid_hll TEXT NOT NULL,
                           identified_anonymousid_hll TEXT NOT NULL
);

CREATE INDEX idx_tracked_users_reports_reported_at ON tracked_users_reports (reported_at);
