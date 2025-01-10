DROP FUNCTION IF EXISTS unionjobsdb(text,integer);

-- change function return table's payload type
CREATE OR REPLACE FUNCTION unionjobsdb(prefix text, num int)
RETURNS table (
  t_name text,
  job_id bigint,
  workspace_id text,
  uuid uuid,
  user_id text,
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
    format('SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.parameters, j.custom_val, (j.event_payload::TEXT), j.event_count, j.created_at, j.expire_at, latest_status.id, latest_status.job_state, latest_status.attempt, latest_status.exec_time, latest_status.error_code, latest_status.error_response FROM %1$I j LEFT JOIN %2$I latest_status on latest_status.job_id = j.job_id', alltables.table_name, 'v_last_' || prefix || '_job_status_'|| substring(alltables.table_name, char_length(prefix)+7,30)),
    ' UNION ') INTO qry
  FROM (select table_name from information_schema.tables
WHERE table_name LIKE prefix || '_jobs_%' order by split_part(split_part(table_name, '_jobs_', 2), '_', 1)::integer asc LIMIT num) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;