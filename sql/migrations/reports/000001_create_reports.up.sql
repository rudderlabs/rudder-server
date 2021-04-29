---
--- Reports
---

CREATE TABLE IF NOT EXISTS reports (
		id BIGSERIAL PRIMARY KEY,
		workspace_id VARCHAR(64) NOT NULL,
		namespace VARCHAR(64) NOT NULL,
		instance_id VARCHAR(64) NOT NULL,
		source_id VARCHAR(64),
		destination_id VARCHAR(64) NOT NULL,
		source_batch_id VARCHAR(64),
		source_task_id VARCHAR(64),
		source_task_run_id VARCHAR(64),
		source_job_id VARCHAR(64),
		source_job_run_id VARCHAR(64),
		in_pu VARCHAR(64),
		pu VARCHAR(64),
		reported_at BIGINT NOT NULL,
		status_code INT,
		sample_response TEXT,
		sample_event JSONB,
		status VARCHAR(64) NOT NULL,
		count BIGINT,
		terminal_state BOOLEAN,
		initial_state BOOLEAN
		);
