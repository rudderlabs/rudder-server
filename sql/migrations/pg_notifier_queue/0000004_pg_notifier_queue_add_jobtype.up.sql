---
--- Operations
---

ALTER TABLE pg_notifier_queue ADD COLUMN IF NOT EXISTS job_type text;