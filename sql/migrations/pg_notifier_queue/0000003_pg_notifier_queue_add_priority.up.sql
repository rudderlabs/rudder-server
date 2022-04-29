---
--- Operations
---

ALTER TABLE pg_notifier_queue ADD COLUMN IF NOT EXISTS priority int;
