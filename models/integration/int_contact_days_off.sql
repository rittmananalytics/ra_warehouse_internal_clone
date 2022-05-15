{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'humaans_hr' in var("hr_warehouse_timeoff_sources") %}

WITH
  date_spine AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2019-01-01', '2025-12-31', INTERVAL 1 DAY)) time_off_calendar_day ),
  person_days_off AS (
  SELECT
    o.contact_id,
    TIMESTAMP(time_off_calendar_day) time_off_calendar_day_ts,
    time_off_type_name,
    CASE
      WHEN o.is_time_off THEN 1
    ELSE
    0
  END
    AS day_off
  FROM
    date_spine d
  LEFT JOIN
    {{ ref('stg_humaans_hr_time_off') }} o
  ON
    TIMESTAMP(time_off_calendar_day) BETWEEN o.time_off_start_ts
    AND o.time_off_end_ts
  ORDER BY
    contact_id,
    time_off_calendar_day)
SELECT
  contact_id,
  time_off_type_name,
  TIMESTAMP_TRUNC(time_off_calendar_day_ts, week) AS time_off_calendar_week,
  SUM(day_off) AS days_off
FROM
  person_days_off
WHERE
  day_off = 1
  AND EXTRACT(dayofweek
  FROM
    DATE(time_off_calendar_day_ts)) BETWEEN 2
  AND 6
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
