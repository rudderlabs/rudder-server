---
--- Operations
---

CREATE TABLE IF NOT EXISTS operations (
		id SERIAL PRIMARY KEY,
		operation VARCHAR(36) NOT NULL,
        payload JSONB NOT NULL,
		done BOOLEAN NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW());