with rt_jobs_view AS (
  SELECT 
    jobs.job_id, 
    jobs.uuid, 
    jobs.user_id, 
    jobs.parameters, 
    jobs.custom_val, 
    jobs.event_payload, 
    jobs.event_count, 
    jobs.created_at, 
    jobs.expire_at, 
    jobs.workspaceid, 
    sum(jobs.event_count) over (
      order by 
        jobs.job_id asc
    ) as running_event_counts, 
    job_latest_state.job_state, 
    job_latest_state.attempt, 
    job_latest_state.exec_time, 
    job_latest_state.retry_time, 
    job_latest_state.error_code, 
    job_latest_state.error_response, 
    job_latest_state.parameters as status_parameters 
  FROM 
    rt_jobs_1 AS jobs LEFT JOIN
    (
      SELECT 
        job_id, 
        job_state, 
        attempt, 
        exec_time, 
        retry_time, 
        error_code, 
        error_response, 
        parameters 
      FROM 
        rt_job_status_1 
      WHERE 
        id IN (
          SELECT 
            MAX(id) 
          from 
            rt_job_status_1 
          GROUP BY 
            job_id
        ) 
    ) AS job_latest_state  
    ON jobs.job_id = job_latest_state.job_id 
  WHERE  
    (   job_latest_state.job_id  = NULL 
            OR
            (
          (job_latest_state.job_state = 'waiting') 
          OR 
          (job_latest_state.job_state = 'failed')
        )
    ) 
    AND
    jobs.workspaceid IN (
      '23HDmhmSjNtmWEKxJ74tMbaI6vh', '1zfesRCw7wm1HHWyijJyH7tbtdY'
    ) 
    AND (
      (jobs.custom_val = 'WEBHOOK')
    ) 
    AND job_latest_state.retry_time < $1
)(
  SELECT 
    jobs.job_id, 
    jobs.uuid, 
    jobs.user_id, 
    jobs.parameters, 
    jobs.custom_val, 
    jobs.event_payload, 
    jobs.event_count, 
    jobs.created_at, 
    jobs.expire_at, 
    jobs.workspaceid, 
    jobs.running_event_counts, 
    jobs.job_state, 
    jobs.attempt, 
    jobs.exec_time, 
    jobs.retry_time, 
    jobs.error_code, 
    jobs.error_response, 
    jobs.status_parameters 
  FROM 
    rt_jobs_view AS jobs 
  WHERE 
    jobs.workspaceid = '23HDmhmSjNtmWEKxJ74tMbaI6vh' 
  ORDER BY 
    jobs.job_id 
  LIMIT 
    4
) 
UNION 
  (
    SELECT 
      jobs.job_id, 
      jobs.uuid, 
      jobs.user_id, 
      jobs.parameters, 
      jobs.custom_val, 
      jobs.event_payload, 
      jobs.event_count, 
      jobs.created_at, 
      jobs.expire_at, 
      jobs.workspaceid, 
      jobs.running_event_counts, 
      jobs.job_state, 
      jobs.attempt, 
      jobs.exec_time, 
      jobs.retry_time, 
      jobs.error_code, 
      jobs.error_response, 
      jobs.status_parameters 
    FROM 
      rt_jobs_view AS jobs 
    WHERE 
      jobs.workspaceid = '1zfesRCw7wm1HHWyijJyH7tbtdY' 
    ORDER BY 
      jobs.job_id 
    LIMIT 
      4
  )






with rt_jobs_view AS (
  SELECT 
    jobs.job_id, 
    jobs.uuid, 
    jobs.user_id, 
    jobs.parameters, 
    jobs.custom_val, 
    jobs.event_payload, 
    jobs.event_count, 
    jobs.created_at, 
    jobs.expire_at, 
    jobs.workspaceid, 
    0 as running_event_counts 
  FROM 
    rt_jobs_1 jobs 
    LEFT JOIN rt_job_status_1 AS job_status ON jobs.job_id = job_status.job_id 
  WHERE 
    job_status.job_id is NULL 
    AND jobs.workspaceid IN (
      '23HDmhmSjNtmWEKxJ74tMbaI6vh', '1zfesRCw7wm1HHWyijJyH7tbtdY'
    ) 
    AND (
      (jobs.custom_val = 'WEBHOOK')
    )
)(
  SELECT 
    jobs.job_id, 
    jobs.uuid, 
    jobs.user_id, 
    jobs.parameters, 
    jobs.custom_val, 
    jobs.event_payload, 
    jobs.event_count, 
    jobs.created_at, 
    jobs.expire_at, 
    jobs.workspaceid, 
    jobs.running_event_counts 
  FROM 
    rt_jobs_view AS jobs 
  WHERE 
    jobs.workspaceid = '23HDmhmSjNtmWEKxJ74tMbaI6vh' 
  ORDER BY 
    jobs.job_id 
  LIMIT 
    4
) 
UNION 
  (
    SELECT 
      jobs.job_id, 
      jobs.uuid, 
      jobs.user_id, 
      jobs.parameters, 
      jobs.custom_val, 
      jobs.event_payload, 
      jobs.event_count, 
      jobs.created_at, 
      jobs.expire_at, 
      jobs.workspaceid, 
      jobs.running_event_counts 
    FROM 
      rt_jobs_view AS jobs 
    WHERE 
      jobs.workspaceid = '1zfesRCw7wm1HHWyijJyH7tbtdY' 
    ORDER BY 
      jobs.job_id 
    LIMIT 
      4
  )
