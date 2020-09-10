---
--- Removing columns that are used to maintain master schema and counters in event_models
---

ALTER TABLE event_models DROP COLUMN IF EXISTS schema;
ALTER TABLE event_models DROP COLUMN IF EXISTS metadata;
ALTER TABLE event_models DROP COLUMN IF EXISTS private_data;
ALTER TABLE event_models DROP COLUMN IF EXISTS total_count;
ALTER TABLE event_models DROP COLUMN IF EXISTS last_seen;
