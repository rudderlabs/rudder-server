CREATE TABLE IF NOT EXISTS drain_config (
		id BIGSERIAL PRIMARY KEY,
		key TEXT NOT NULL,
		value TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
