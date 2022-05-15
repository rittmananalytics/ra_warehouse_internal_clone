{% if var("projects_warehouse_forecast_sources") %}

with t_timesheets_forecast as
  (
    {% for source in var('projects_warehouse_forecast_sources') %}
      {% set relation_source = 'stg_' + source + '_timesheets_forecast' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
,
date_spine as (SELECT
  *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2019-01-01', '2025-12-31', INTERVAL 1 DAY)) forecast_day
    ),
forecast_hours as (
select
  o.contact_id as contact_id,
  o.timesheet_project_id,
  timestamp(forecast_day) as forecast_day_ts,
  hours_forecast / 8 as forecast_days
from
  date_spine d
left join
  t_timesheets_forecast o
on
  timestamp(forecast_day) between o.forecast_start_ts and o.forecast_end_ts
)
select
  contact_id,
  timesheet_project_id,
  forecast_day_ts,
  sum(forecast_days) as forecast_days
from
  forecast_hours
where
  forecast_days is not null
group by 1,2,3

{% else %} {{config(enabled=false)}} {% endif %}
