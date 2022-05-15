{% if var("projects_warehouse_forecast_sources") %}

SELECT f.contact_id,
      timestamp(date_trunc(date(forecast_day_ts),week)) as forecast_week,
      sum(forecast_days) as forecast_billable_days,
      max(contact_weekly_capacity/60/60) total_billable_capacity
FROM
{{ ref('int_timesheets_forecast') }} f
join
{{ ref('stg_harvest_forecast_contacts') }} c
on f.contact_id = c.contact_id

group by 1,2

{% else %} {{config(enabled=false)}} {% endif %}
