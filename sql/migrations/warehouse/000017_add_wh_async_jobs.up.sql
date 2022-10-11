--
-- wh_staging_files
--

CREATE TABLE IF NOT EXISTS wh_async_jobs (
    id BIGSERIAL PRIMARY KEY,
    source_id character varying(64) NOT NULL,
    destination_id character varying(64) NOT NULL,
    status character varying(64) NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    tablename text NOT NULL,
    error text,
    async_job_type character varying(64) NOT NULL,
    metadata jsonb,
    attempt integer DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS asyncjobindex ON wh_async_jobs (source_id,destination_id, (metadata->>'job_run_id'),(metadata->>'task_run_id'),tablename) WHERE metadata->>'job_run_id'!='' AND metadata->>'task_run_id'!='' ;
