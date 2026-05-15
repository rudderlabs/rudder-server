-- unionjobsdb function automatically joins datasets and returns jobs
-- along with their latest jobs status (or null)
--
-- Parameters
-- prefix: table prefix, e.g. gw, rt, batch_rt
-- num: number of datasets to include in the query, e.g. 4
CREATE OR REPLACE FUNCTION unionjobsdb(prefix text, num int)
RETURNS table (
  t_name text,
  job_id bigint,
  workspace_id text,
  uuid uuid,
  user_id text,
  partition_id text,
  parameters jsonb,
  custom_val character varying(64),
  event_payload text,
  event_count integer,
  created_at timestamp with time zone,
  expire_at timestamp with time zone,
  status_id bigint,
  job_state character varying(64),
  attempt smallint,
  exec_time timestamp with time zone,
  error_code character varying(32),
  error_response jsonb
)
AS $$
DECLARE
  qry text;
BEGIN
SELECT string_agg(
    format('SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.partition_id, j.parameters, j.custom_val, j.event_payload, j.event_count, j.created_at, j.expire_at, latest_status.id, latest_status.job_state, latest_status.attempt, latest_status.exec_time, latest_status.error_code, latest_status.error_response FROM %1$I j LEFT JOIN %2$I latest_status on latest_status.job_id = j.job_id', alltables.table_name, 'v_last_' || prefix || '_job_status_'|| substring(alltables.table_name, char_length(prefix)+7,30)),
    ' UNION ') INTO qry
  FROM (
    select c.relname as table_name
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_description d on d.objoid = c.oid and d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relname LIKE prefix || '_jobs_%'
      AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
    order by c.relname asc LIMIT num
  ) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;


-- unionjobsdbmetadata function automatically joins datasets and returns jobs
-- along with their latest jobs status (or null) excluding the payload
--
-- Parameters
-- prefix: table prefix, e.g. gw, rt, batch_rt
-- num: number of datasets to include in the query, e.g. 4
CREATE OR REPLACE FUNCTION unionjobsdbmetadata(prefix text, num int)
RETURNS table (
  t_name text,
  job_id bigint,
  workspace_id text,
  uuid uuid,
  user_id text,
  partition_id text,
  parameters jsonb,
  custom_val character varying(64),
  event_count integer,
  created_at timestamp with time zone,
  expire_at timestamp with time zone,
  status_id bigint,
  job_state character varying(64),
  attempt smallint,
  exec_time timestamp with time zone,
  error_code character varying(32),
  error_response jsonb
)
AS $$
DECLARE
  qry text;
BEGIN
SELECT string_agg(
    format('SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.partition_id, j.parameters, j.custom_val, j.event_count, j.created_at, j.expire_at, latest_status.id, latest_status.job_state, latest_status.attempt, latest_status.exec_time, latest_status.error_code, latest_status.error_response FROM %1$I j LEFT JOIN %2$I latest_status on latest_status.job_id = j.job_id', alltables.table_name, 'v_last_' || prefix || '_job_status_'|| substring(alltables.table_name, char_length(prefix)+7,30)),
    ' UNION ') INTO qry
  FROM (
    select c.relname as table_name
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_description d on d.objoid = c.oid and d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relname LIKE prefix || '_jobs_%'
      AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
    order by c.relname asc LIMIT num
  ) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;


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
  FROM (
    select c.relname as table_name
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_description d on d.objoid = c.oid and d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relname LIKE 'rt_jobs_%'
      AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
    order by c.relname asc LIMIT num
  ) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;
