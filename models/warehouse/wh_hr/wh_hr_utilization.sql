{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'humaans_hr' in var("hr_warehouse_timeoff_sources") %}

{{
    config(
        alias='contact_utilization_fact'
    )
}}


WITH
  forecast_weeks AS (
  SELECT * from (
  SELECT
    c.contact_pk,
    c.contact_name,
    DATE_TRUNC(forecast_day_ts,week) AS forecast_week,
    SUM(forecast_days) AS forecast_days
  FROM
    {{ ref('wh_timesheet_project_forecast')}} f
  JOIN
    {{ ref('wh_contacts_dim')}} c
  ON
    f.contact_pk = c.contact_pk
  GROUP BY
    1,
    2,
    3 )
  GROUP BY 1,2,3,4),
  days_off AS (
  SELECT
    contact_pk,
    DATE_TRUNC(time_off_calendar_week,week) AS forecast_week,
    SUM(days_off) AS days_off
  FROM
    {{ ref('wh_hr_days_off_fact')}} d
  GROUP BY
    1,
    2 ),
  actual_billable_hours AS (
  SELECT
    contact_pk,
    TIMESTAMP(DATE_TRUNC(DATE(timesheet_billing_date),week)) AS timesheet_billing_week,
    SUM(timesheet_hours_billed) AS actual_billable_hours
  FROM
    {{ ref('wh_timesheets_fact')}}
  WHERE
    timesheet_is_billable
  GROUP BY
    1,
    2),
  delivered_story_points AS (
  SELECT
    contact_pk,
    TIMESTAMP(DATE_TRUNC(DATE(task_status_change_ts),week)) AS delivery_week,
    SUM(task_story_points) AS total_story_points
  FROM
    {{ ref('wh_delivery_tasks_fact')}}
  GROUP BY
    1,
    2),
  numbers AS (
  SELECT
    coalesce(coalesce(w.contact_pk,
        d.contact_pk),
      a.contact_pk) AS contact_pk,
    contact_name,
    coalesce(coalesce(w.forecast_week,
        a.timesheet_billing_week),
      d.forecast_week) AS forecast_week,
    coalesce(forecast_days,
      0) AS forecast_days,
    coalesce(days_off*8,
      0) AS days_off,
    coalesce(a.actual_billable_hours,
      0) AS actual_billable_hours,
    coalesce(total_story_points,
      0) AS actual_story_points
  FROM
    forecast_weeks w
  FULL OUTER JOIN
    days_off d
  ON
    w.contact_pk = d.contact_pk
    AND w.forecast_week = d.forecast_week
  FULL OUTER JOIN
    actual_billable_hours a
  ON
    w.contact_pk = a.contact_pk
    AND w.forecast_week = a.timesheet_billing_week
  LEFT JOIN
    delivered_story_points s
  ON
    w.contact_pk = s.contact_pk
    AND w.forecast_week = s.delivery_week)
SELECT
  *,
  CASE
    WHEN contact_name = 'Mark Rittman' THEN .4
    WHEN contact_name = 'Lewis Baker' THEN .6
  ELSE
  .75
END
  AS target,
  40 AS hours_per_week,
  40-days_off AS total_capacity,
  (40-days_off) * (CASE
      WHEN contact_name = 'Mark Rittman' THEN .4
      WHEN contact_name = 'Lewis Baker' THEN .6
    ELSE
    .75
  END
    ) AS target_billable_capacity,
  forecast_days*8 AS forecast_billable_hours,
  safe_divide((forecast_days*8),(40-days_off)) * (CASE
      WHEN contact_name = 'Mark Rittman' THEN .4
      WHEN contact_name = 'Lewis Baker' THEN .6
    ELSE
    .75
  END
    ) AS forecast_utilization
FROM
  numbers
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
