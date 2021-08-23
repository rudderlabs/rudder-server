-- wh_load_files --

ALTER TABLE wh_load_files ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';
