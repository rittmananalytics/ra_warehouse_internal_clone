{% if var('crm_warehouse_conversations_sources') %}

SELECT
  CONCAT('hubspot-',CAST(engagement_id AS string)) as engagement_id,
  type AS engagment_type,
  owner_contact_id AS from_contact_id,
  createdat AS engagement_ts,
  title AS engagement_title,
  deal_id,
  contact_id AS to_contact_id
FROM
  {{ ref('stg_hubspot_crm_meetings') }},
  UNNEST(contact_ids) contact_id
UNION ALL
SELECT
  conversation_id AS engagement_id,
  conversation_message_type AS engagment_type,
  c.contact_id AS from_contact_id,
  conversation_created_date AS engagement_ts,
  conversation_subject AS engagement_title,
  deal_id,
  c2.contact_id AS to_contact_id
FROM
  {{ ref('stg_hubspot_crm_conversations') }} co
JOIN
  {{ ref('stg_hubspot_crm_contacts') }} c
ON
  co.conversation_from_contact_name = c.contact_name
JOIN
  {{ ref('stg_hubspot_crm_contacts') }} c2
ON
  co.conversation_to_email = c2.contact_email

{% else %} {{config(enabled=false)}} {% endif %}
