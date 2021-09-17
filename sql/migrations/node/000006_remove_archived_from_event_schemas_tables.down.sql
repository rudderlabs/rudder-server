---
--- Removing colum `archived` from event_models and schema_versions tables
---

ALTER TABLE event_models DROP COLUMN IF EXISTS archived;
ALTER TABLE schema_versions DROP COLUMN IF EXISTS archived;
