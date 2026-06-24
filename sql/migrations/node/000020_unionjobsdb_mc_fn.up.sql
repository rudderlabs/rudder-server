-- unionjobsdb function automatically joins datasets and returns jobs
-- along with their latest jobs status (or null)
--
-- Parameters
-- prefix: table prefix, e.g. gw, rt, batch_rt
-- num: number of datasets to include in the query, e.g. 4
DROP FUNCTION IF EXISTS unionjobsdb(text, integer);
CREATE FUNCTION unionjobsdb(prefix text, num int)
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
  consumers text[],
  status_id bigint,
  consumer text,
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
    format(
      'SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.partition_id, j.parameters, j.custom_val, j.event_payload, j.event_count, j.created_at, j.expire_at, j.consumers, s.id, s.consumer, s.job_state, s.attempt, s.exec_time, s.error_code, s.error_response'
      ' FROM %1$I j LEFT JOIN %2$I s ON s.job_id = j.job_id',
      alltables.table_name,
      CASE
        WHEN to_regclass(prefix || '_consumers_' || substring(alltables.table_name, char_length(prefix)+7, 30)) IS NOT NULL
        THEN 'v_last_c_' || prefix || '_job_status_' || substring(alltables.table_name, char_length(prefix)+7, 30)
        ELSE 'v_last_'   || prefix || '_job_status_' || substring(alltables.table_name, char_length(prefix)+7, 30)
      END
    ),
    ' UNION ') INTO qry
  FROM (
    SELECT c.relname AS table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relname LIKE prefix || '_jobs_%'
      AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
    ORDER BY c.relname ASC LIMIT num
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
DROP FUNCTION IF EXISTS unionjobsdbmetadata(text, integer);
CREATE FUNCTION unionjobsdbmetadata(prefix text, num int)
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
  consumers text[],
  status_id bigint,
  consumer text,
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
    format(
      'SELECT %1$L, j.job_id, j.workspace_id, j.uuid, j.user_id, j.partition_id, j.parameters, j.custom_val, j.event_count, j.created_at, j.expire_at, j.consumers, s.id, s.consumer, s.job_state, s.attempt, s.exec_time, s.error_code, s.error_response'
      ' FROM %1$I j LEFT JOIN %2$I s ON s.job_id = j.job_id',
      alltables.table_name,
      CASE
        WHEN to_regclass(prefix || '_consumers_' || substring(alltables.table_name, char_length(prefix)+7, 30)) IS NOT NULL
        THEN 'v_last_c_' || prefix || '_job_status_' || substring(alltables.table_name, char_length(prefix)+7, 30)
        ELSE 'v_last_'   || prefix || '_job_status_' || substring(alltables.table_name, char_length(prefix)+7, 30)
      END
    ),
    ' UNION ') INTO qry
  FROM (
    SELECT c.relname AS table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relname LIKE prefix || '_jobs_%'
      AND COALESCE(d.description, '') NOT LIKE 'rudder:pre_drop:%'
    ORDER BY c.relname ASC LIMIT num
  ) alltables;
RETURN QUERY EXECUTE qry;
END;
$$ LANGUAGE plpgsql;
