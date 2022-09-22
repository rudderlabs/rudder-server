--
-- wh_staging_files
--

CREATE TABLE wh_async_jobs (
    id BIGSERIAL PRIMARY KEY,
    sourceid character varying(64) NOT NULL,
    destinationid character varying(64) NOT NULL,
    status character varying(64) NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    tablename text NOT NULL,
    error text,
    async_job_type character varying(64) NOT NULL,
    metadata jsonb,
    attempt integer DEFAULT 0
);

CREATE UNIQUE INDEX asyncjobindex ON wh_async_jobs (sourceid,destinationid, (metadata->>'jobrunid'),(metadata->>'taskrunid'),tablename) WHERE metadata->>'jobrunid'!='' AND metadata->>'taskrunid'!='' ;
