{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") and var("crm_warehouse_contact_sources") %}
{{
    config(
        alias='contact_engagements_fact'
    )
}}

with contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
  deals as (
    SELECT
      deal_pk,
      deal_id
    FROM
      {{ ref('wh_deals_fact') }}
    ),
  engagements as (
  SELECT
    *
  FROM
    {{ ref('int_engagements') }}
  )
select
      {{ dbt_utils.surrogate_key(
      ['engagement_id']
    ) }} as engagement_pk,
       c.contact_pk as from_contact_pk,
       c2.contact_pk as to_contact_pk,
       e.* except (from_contact_id, to_contact_id, deal_id, engagement_id),
       d.deal_pk
from   engagements e
join   contacts_dim c
on     e.from_contact_id = c.contact_id
left join   contacts_dim c2
on     e.to_contact_id = c2.contact_id
left join deals d
on     e.deal_id = d.deal_id

{% else %} {{config(enabled=false)}} {% endif %}
