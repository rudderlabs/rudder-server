---
--- Adding columns to maintain master schema and counters in event_models
---

ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS schema JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS private_data JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS total_count BIGINT DEFAULT 0;
ALTER TABLE event_models
		ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP NOT NULL DEFAULT NOW();