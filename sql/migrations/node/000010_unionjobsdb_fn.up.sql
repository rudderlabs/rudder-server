DROP FUNCTION IF EXISTS unionjobsdb(text,integer);
DROP FUNCTION IF EXISTS unionjobsdbmetadata(text,integer);

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
  parameters jsonb,
  custom_val character varying(64),
  event_payload jsonb,
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
    format('SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.parameters, j.custom_val, j.event_payload, j.event_count, j.created_at, j.expire_at, latest_status.id, latest_status.job_state, latest_status.attempt, latest_status.exec_time, latest_status.error_code, latest_status.error_response FROM %1$I j LEFT JOIN %2$I latest_status on latest_status.job_id = j.job_id', alltables.table_name, 'v_last_' || prefix || '_job_status_'|| substring(alltables.table_name, char_length(prefix)+7,30)),
    ' UNION ') INTO qry
  FROM (select table_name from information_schema.tables
WHERE table_name LIKE prefix || '_jobs_%' order by table_name asc LIMIT num) alltables;
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
    format('SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.parameters, j.custom_val, j.event_count, j.created_at, j.expire_at, latest_status.id, latest_status.job_state, latest_status.attempt, latest_status.exec_time, latest_status.error_code, latest_status.error_response FROM %1$I j LEFT JOIN %2$I latest_status on latest_status.job_id = j.job_id', alltables.table_name, 'v_last_' || prefix || '_job_status_'|| substring(alltables.table_name, char_length(prefix)+7,30)),
    ' UNION ') INTO qry
  FROM (select table_name from information_schema.tables
WHERE table_name LIKE prefix || '_jobs_%' order by table_name asc LIMIT num) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;