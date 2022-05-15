{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_content_interaction_sources") %}
{% if 'linkedin_followers' in var("marketing_warehouse_content_interaction_sources") %}

WITH interactions AS (
  select
    *
  from
    {{ source('linkedin_followers','contacts' ) }}
  )
,
renamed as (
SELECT
  concat('{{ var('stg_linkedin_social_id-prefix') }}',concat(REGEXP_REPLACE(firstName, r'\W', ''),' ',REGEXP_REPLACE(lastName, r'\W', ''))) as contact_id,
  cast(null as string) as interaction_content_id,
  cast(null as string) as interaction_content_url,
  cast(null as string) as interaction_content_title,
  'follow' as interaction_event_name,
  cast(null as string) as interaction_event_details,
  parse_timestamp('%b %Y',followedAt) as interaction_event_ts
FROM
  interactions
)
select
  *
from
  renamed c


  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
