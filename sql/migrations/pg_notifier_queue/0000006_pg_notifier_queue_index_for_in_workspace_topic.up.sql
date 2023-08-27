---
--- Operations
---

CREATE INDEX IF NOT EXISTS pg_notifier_queue_workspace_idx ON pg_notifier_queue (workspace);
CREATE INDEX IF NOT EXISTS pg_notifier_queue_topic_idx ON pg_notifier_queue (topic);
