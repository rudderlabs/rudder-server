CREATE TABLE activation_records_reports (
                           id BIGSERIAL PRIMARY KEY,
                           workspace_id VARCHAR(64) NOT NULL,
                           instance_id VARCHAR(64) NOT NULL,
                           source_id VARCHAR(64) NOT NULL,
                           destination_id VARCHAR(64) NOT NULL,
                           reported_at TIMESTAMPTZ NOT NULL,
                           fingerprint_hll TEXT NOT NULL
);

CREATE INDEX idx_activation_records_reports_reported_at ON activation_records_reports (reported_at);
