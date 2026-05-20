ALTER TABLE config_cache ADD COLUMN IF NOT EXISTS config_hash text;

ALTER TABLE config_cache
SET (
	toast.autovacuum_vacuum_scale_factor = 0.0,
	toast.autovacuum_vacuum_threshold = 1
);
