ALTER TABLE error_detail_reports
    ALTER COLUMN sample_event TYPE TEXT,
    ALTER COLUMN sample_event SET DEFAULT '{}';
