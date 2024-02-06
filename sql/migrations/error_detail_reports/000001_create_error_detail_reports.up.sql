---
--- Error Detail Reports
---

CREATE TABLE IF NOT EXISTS error_detail_reports (
		id BIGSERIAL PRIMARY KEY,
		workspace_id VARCHAR(64) NOT NULL,
		namespace VARCHAR(64) NOT NULL,
		instance_id VARCHAR(64) NOT NULL,
		source_id VARCHAR(64),
		source_definition_id TEXT DEFAULT '',
		destination_id VARCHAR(64) NOT NULL,
		destination_definition_id TEXT DEFAULT '',
		dest_type TEXT DEFAULT '',
		pu VARCHAR(64),
		reported_at BIGINT NOT NULL,
		status_code INT,
		error_message TEXT DEFAULT '',
		event_type TEXT DEFAULT '',
		error_code TEXT DEFAULT '',
		count BIGINT DEFAULT 0
		);