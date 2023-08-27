---
--- Operations
---

CREATE TABLE IF NOT EXISTS pg_notifier_queue_metadata (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    metadata JSONB NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS pg_notifier_queue_metadata_uniq_id ON pg_notifier_queue_metadata (batch_id);
