{% if var("marketing_warehouse_meetings_sources")  %}

{{
    config(
        alias='marketing_conversions_fact'
    )
}}


with contacts as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
meetings as (
  select *
  from {{ ref('int_marketing_conversions') }}


)
select {{ dbt_utils.surrogate_key(['m.meeting_id','m.contact_id','m.meeting_ts']) }} as marketing_conversion_pk,
       c.contact_pk,
       m.* except(_dbt_source_relation, contact_name, contact_email, meeting_id, contact_id)
from meetings m
left join
    contacts c
on  m.contact_id = c.contact_id

{% else %} {{config(enabled=false)}} {% endif %}
