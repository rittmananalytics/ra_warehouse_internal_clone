{% if var("projects_warehouse_forecast_sources") %}
{{
    config(
        unique_key='timesheet_forecast_pk',
        alias='timesheets_forecast_fact'
    )
}}


WITH forecast AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_timesheets_forecast')) }}
  FROM   {{ ref('int_timesheets_forecast') }}
),
contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
  projects_dim AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('wh_timesheet_projects_dim')
      ) }}
    FROM
      {{ ref('wh_timesheet_projects_dim') }}
  )
SELECT
{{ dbt_utils.surrogate_key(['c.contact_pk','p.timesheet_project_pk','forecast_day_ts']) }} as timesheet_forecast_pk,
 c.contact_pk,
 p.timesheet_project_pk,
   f.* except (timesheet_project_id, contact_id)
FROM
   forecast f
join   contacts_dim c
on     f.contact_id = c.contact_id
left join projects_dim p
ON f.timesheet_project_id = p.timesheet_project_id
WHERE p.project_is_billable
group by
  1,2,3,4,5
{% else %}

{{config(enabled=false)}}

{% endif %}
