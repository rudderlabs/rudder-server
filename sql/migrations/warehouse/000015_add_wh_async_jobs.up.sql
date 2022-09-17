--
-- wh_staging_files
--

CREATE TABLE wh_async_jobs (
    id SERIAL PRIMARY KEY,
    sourceid VARCHAR(64) NOT NULL,
    destinationid VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    jobrunid VARCHAR(64) NOT NULL,
    starttime timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    destination_type VARCHAR(64) NOT NULL,
    taskrunid VARCHAR(64) NOT NULL,
    tablename text NOT NULL,
    jobtype VARCHAR(64) NOT NULL,
    namespace VARCHAR(64) NOT NULL,
    error text,
    async_job_type VARCHAR(64) NOT NULL,
    UNIQUE(sourceid,destinationid,jobrunid,taskrunid,tablename)
);