{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}


{% if var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_engagements_table'),unique_column='engagement_id') }}
),
renamed as (
  SELECT
    engagement_id,
    engagement.type ,
    engagement.ownerid,
    concat(o.owner_first_name,' ',o.owner_last_name) as meeting_owner_contact_name,
    c.contact_id as owner_contact_id,
    engagement.createdat,
    metadata.title as title,
    concat('hubspot-',companyids.value) as company_id,
    array_agg(distinct concat('hubspot-',contactids.value)) as contact_ids,
    dealids.value as deal_id
  FROM
    source e,
    UNNEST (associations.companyids) companyids,
    UNNEST (associations.contactids) contactids,
    UNNEST (associations.dealids) dealids
  JOIN {{ ref('stg_hubspot_crm_owners') }} o
    on e.engagement.ownerid = o.owner_id
  JOIN {{ ref('stg_hubspot_crm_contacts') }} c
    on o.owner_first_name = c.contact_first_name
    and o.owner_last_name = c.contact_last_name
    where
     engagement.type = 'MEETING'
    group by 1,2,3,4,5,6,7,8,10
{% endif %}
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
