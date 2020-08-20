---
--- Add Column to store counters data
---

ALTER TABLE schema_versions 
		ADD COLUMN IF NOT EXISTS private_data JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE schema_versions
		ADD CONSTRAINT unique_uuid UNIQUE (uuid);
