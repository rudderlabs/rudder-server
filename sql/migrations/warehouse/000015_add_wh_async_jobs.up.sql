--
-- wh_staging_files
--

CREATE TABLE wh_async_jobs (
    id SERIAL PRIMARY KEY,
    sourceid text,
    destinationid text,
    status text,
    jobrunid text,
    starttime text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    destination_type text,
    taskrunid text,
    tablename text,
    jobtype text,
    namespace text,
    error text,
    async_job_type text,
    UNIQUE(sourceid,destinationid,jobrunid,taskrunid,tablename)
);