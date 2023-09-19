DROP FUNCTION IF EXISTS joborderlog(text,text,integer);

-- joborderlog function automatically joins datasets and returns all router job statuses 
-- for a given destination_id and user_id
--
-- Parameters
-- destinationid: the destination id
-- userid: the user id
-- num: maximum number of datasets to include in the query, e.g. 4
CREATE OR REPLACE FUNCTION joborderlog(destinationid text, userid text, num int)
RETURNS table (
  t_name text,
  job_id bigint,
  created_at timestamp with time zone,
  status_id bigint,
  job_state character varying(64),
  attempt smallint,
  exec_time timestamp with time zone,
  error_code character varying(32),
  parameters jsonb,
  error_response jsonb
)
AS $$
DECLARE
  qry text;
BEGIN
SELECT 'SELECT q.* FROM (' || string_agg(
    format('SELECT %1$L, j.job_id, j.created_at, js.id, js.job_state, js.attempt, js.exec_time, js.error_code, js.parameters, js.error_response FROM %1$I j LEFT JOIN %2$I js on js.job_id = j.job_id WHERE j.parameters->>''destination_id'' = %3$L AND j.user_id = %4$L', alltables.table_name, 'rt_job_status_'|| substring(alltables.table_name, 9,30), destinationid, userid),
    ' UNION ') || ') q ORDER BY q.exec_time, q.id' INTO qry
  FROM (select table_name from information_schema.tables
WHERE table_name LIKE 'rt_jobs_%' order by table_name asc LIMIT num) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;