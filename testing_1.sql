
WITH event_timestamps as
(
select distinct DATETIME_TRUNC(timestamp(`timestamp`),HOUR) as trans_hr from  `${aether_5g_core_module_src_project_id}.${aether_5g_core_module_src_dataset_name}.${aether_5g_core_module_src_upf_tblname}`
  where DATETIME_TRUNC(timestamp(insert_date_utc),HOUR) in
  unnest(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_SUB(DATETIME_TRUNC(timestamp(trans_ts),HOUR),INTERVAL window_interval-1 HOUR),DATETIME_TRUNC(timestamp(trans_ts),HOUR),INTERVAL window_hour HOUR))
--  and trans_dt=date(timestamp(trans_ts))
   and trans_dt is not null-- need to check
),

  base_data AS (
  select *,
  MD5(labels) as checksum
  from (
  SELECT
  distinct
    TIMESTAMP_SECONDS(CAST(FLOOR(UNIX_SECONDS(`timestamp`)/(window_hour*60*60)) * (window_hour*60*60) AS INT64)) AS event_time,
    fqdn,
TO_JSON_STRING(JSON_REMOVE(SAFE.PARSE_JSON(labels),'$.__name__','$.jobid','$.localdn')) as labels,
--    JSON_VALUE(labels,'$.instance') as instance,
    lower(name) as KEY,
    SAFE_CAST(nullif(value,'NaN') AS FLOAT64) AS value,
    DATE(`timestamp`) AS trans_dt,
    `timestamp`,
  FROM
    `${aether_5g_core_module_src_project_id}.${aether_5g_core_module_src_dataset_name}.${aether_5g_core_module_src_upf_tblname}`
  WHERE
    DATETIME_TRUNC(timestamp(`timestamp`),HOUR) in (select trans_hr from event_timestamps)
    and insert_date_utc > (select min(trans_hr) from event_timestamps)
and trans_dt in (select date(trans_hr) from event_timestamps)
and trans_dt is not null
)
),
window_data AS (
select *,if(value<prev_value,1,0) has_reset from (
select
   trans_dt,
   event_time,
   fqdn,
   labels,
   checksum,
   KEY,
   value,
   `timestamp`,
    SAFE_CAST(LAG(value) OVER (PARTITION BY fqdn, checksum, KEY ORDER BY `timestamp`) AS FLOAT64) AS prev_value
from base_data
)
),

  reset_adjusted AS (
  SELECT
    event_time,
    fqdn,
--    instance,
    trans_dt,
    MAX(labels) AS labels,
    checksum,
    KEY,
    sum(value) as sum_value,
    SUM(CASE
        WHEN prev_value is NULL THEN 0
        WHEN has_reset =1 THEN value
        ELSE value - IFNULL(prev_value,0)
    END
      ) AS increase_value
  FROM
    window_data
  GROUP BY
  trans_dt,
    event_time,
--    instance,
    fqdn,
   checksum,
    KEY )
SELECT
  event_time,
  fqdn,
  trans_dt,
  TO_JSON(JSON_QUERY(labels,'$')) AS labels,
--  instance,
  checksum,
  KEY,
  increase_value,
  sum_value,
  increase_value/(window_hour*60*60) as rate_value,
FROM
  reset_adjusted
ORDER BY
  trans_dt,
    event_time,
--    instance,
    fqdn,
   checksum,
    KEY
