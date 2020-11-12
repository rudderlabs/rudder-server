
--
-- wh_table_uploads
--

ALTER TABLE wh_table_uploads ADD COLUMN IF NOT EXISTS location TEXT;

ALTER TABLE wh_table_uploads ALTER column total_events TYPE BIGINT USING CAST(total_events AS BIGINT);
