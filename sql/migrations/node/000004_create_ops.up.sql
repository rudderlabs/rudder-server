---
--- Operations
---

CREATE TABLE IF NOT EXISTS operations (
		id SERIAL PRIMARY KEY,
		operation VARCHAR(128) NOT NULL,
        payload JSONB NOT NULL,
		done BOOLEAN NOT NULL,
		op_status VARCHAR(128),
		created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() at time zone 'utc'),
		start_time TIMESTAMP WITHOUT TIME ZONE,
		end_time TIMESTAMP WITHOUT TIME ZONE);