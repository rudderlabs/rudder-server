---
--- Operations
---

ALTER TABLE operations ADD COLUMN IF NOT EXISTS parameters JSONB DEFAULT '{}'::JSONB;
