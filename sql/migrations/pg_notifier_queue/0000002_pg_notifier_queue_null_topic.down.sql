---
--- Operations
---

CREATE INDEX IF NOT EXISTS pg_notifier_queue_workspace_topic_idx ON pg_notifier_queue (workspace, topic);

CREATE INDEX IF NOT EXISTS pg_notifier_queue_status_workspace_idx ON pg_notifier_queue (status, workspace);

ALTER TABLE pg_notifier_queue ALTER COLUMN topic SET NOT NULL;