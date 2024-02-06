---
--- Error Detail Reports
---
ALTER TABLE
    error_detail_reports
ADD
    COLUMN IF NOT EXISTS event_name TEXT DEFAULT '',
ADD
    COLUMN IF NOT EXISTS sample_event JSONB,
ADD
    COLUMN IF NOT EXISTS sample_response TEXT DEFAULT '';