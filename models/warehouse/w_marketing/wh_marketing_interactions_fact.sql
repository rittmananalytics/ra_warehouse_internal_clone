{% if var("marketing_warehouse_content_interaction_sources")  %}

{{
    config(
        alias='marketing_interactions_fact'
    )
}}


with content as (
      select *
      from {{ ref('wh_marketing_content_dim') }}
),contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
interactions as (
  select *
  from {{ ref('int_marketing_content_interactions') }}


)
select {{ dbt_utils.surrogate_key(['i.interaction_content_id','i.contact_id','i.interaction_event_name']) }} as marketing_interaction_pk,
       c.marketing_content_pk,
       d.contact_pk,
       i.* except(_dbt_source_relation, interaction_content_title, interaction_content_url)
from interactions i
left join
    content c
on  i.interaction_content_id = c.interaction_content_id
left join   contacts_dim d
on     i.contact_id = d.contact_id




{% else %} {{config(enabled=false)}} {% endif %}
