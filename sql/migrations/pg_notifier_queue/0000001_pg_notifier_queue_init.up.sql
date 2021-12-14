---
--- Operations
---

DO $$ BEGIN
    CREATE TYPE pg_notifier_status_type
	AS ENUM(
		'waiting',
		'executing',
		'succeeded',
		'failed',
		'aborted'
			);
	EXCEPTION
		WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pg_notifier_queue (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    status pg_notifier_status_type NOT NULL,
    topic VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_exec_time TIMESTAMP,
    attempt SMALLINT DEFAULT 0,
    error TEXT,
    worker_id VARCHAR(64),
    workspace VARCHAR(64),
    priority int);

CREATE INDEX IF NOT EXISTS pg_notifier_queue_status_idx ON pg_notifier_queue (status);

CREATE INDEX IF NOT EXISTS pg_notifier_queue_batch_id_idx ON pg_notifier_queue (batch_id);

CREATE INDEX IF NOT EXISTS pg_notifier_queue_workspace_topic_idx ON pg_notifier_queue (workspace, topic);

CREATE INDEX IF NOT EXISTS pg_notifier_queue_status_workspace_idx ON pg_notifier_queue (status, workspace);

DO $$ BEGIN
    CREATE TYPE pg_notifier_subscriber_status
	AS ENUM(
		'busy',
		'free' );
	EXCEPTION
		WHEN duplicate_object THEN null;
END $$;