---
--- Add Column to store counters data
---

ALTER TABLE schema_versions DROP COLUMN IF EXISTS private_data;
ALTER TABLE schema_versions DROP CONSTRAINT unique_uuid;
ALTER TABLE schema_versions DROP COLUMN IF EXISTS total_count;