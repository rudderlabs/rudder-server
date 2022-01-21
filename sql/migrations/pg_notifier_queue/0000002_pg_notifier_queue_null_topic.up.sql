---
--- Operations
---

DROP INDEX IF EXISTS pg_notifier_queue_workspace_topic_idx;

DROP INDEX IF EXISTS pg_notifier_queue_status_workspace_idx;

ALTER TABLE pg_notifier_queue ALTER COLUMN topic DROP NOT NULL;