{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'humaans_hr' in var("hr_warehouse_timeoff_sources") %}

{{
    config(
        alias='contact_days_off_fact'
    )
}}


WITH days_off AS
  (
  SELECT * from {{ ref('int_contact_days_off') }}
),contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  )
select {{ dbt_utils.surrogate_key(['contact_pk','time_off_type_name','time_off_calendar_week']) }} as time_off_pk,
       c.contact_pk,
       d.* except (contact_id)
from days_off d
join   contacts_dim c
on     d.contact_id = c.contact_id


{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
