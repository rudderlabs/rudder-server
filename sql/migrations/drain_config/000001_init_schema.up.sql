CREATE TABLE IF NOT EXISTS drain_config (
		id BIGSERIAL PRIMARY KEY,
		key TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		value TEXT NOT NULL
);
