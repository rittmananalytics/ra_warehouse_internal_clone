{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources")
  and 'harvest_forecast' in var("projects_warehouse_forecast_sources")
  and 'harvest_forecast' in var("crm_warehouse_contact_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_harvest_forecast_timesheets_forecast_table'),unique_column='id') }}
),
people as (
  {{ filter_stitch_relation(relation=var('stg_harvest_forecast_contacts_table'),unique_column='harvest_user_id') }}
),
projects as (
  {{ filter_stitch_relation(relation=var('stg_harvest_forecast_projects_table'),unique_column='harvest_id') }}
),
renamed as (
select
  s.id  as timesheet_forecast_id,
  cast(p.harvest_id as string) as timesheet_project_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(c.harvest_user_id as string)) as contact_id,
  placeholder_id  as placeholder_id,
  s.start_date  as forecast_start_ts,
  s.end_date  as forecast_end_ts,
  s.allocation / 60 / 60 as hours_forecast,
  s.notes as forecast_notes
from
  source s
join
  people c
on
  s.person_id = c.id
join
  projects p
on
  s.project_id = p.id
)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
