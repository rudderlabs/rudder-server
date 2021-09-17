---
--- Add column `archived` to event_models and schema_versions tables
---

ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS archived BOOL NOT NULL DEFAULT false;
ALTER TABLE schema_versions
		ADD COLUMN IF NOT EXISTS archived BOOL NOT NULL DEFAULT false;
