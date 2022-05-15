{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_content_interaction_sources") %}
{% if 'linkedin_likers' in var("marketing_warehouse_content_interaction_sources") %}

WITH interactions AS (
  select
    *
  from
    {{ source('linkedin_likers','contacts' ) }}
  )
,
renamed as (
SELECT
  concat('{{ var('stg_linkedin_social_id-prefix') }}',concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', ''))) as contact_id,
  concat('{{ var('stg_linkedin_social_id-prefix') }}',split(split(split(postUrl,'activity-')[safe_offset(1)],'?')[safe_offset(0)],'-')[safe_offset(0)]) as interaction_content_id,
  postUrl as interaction_content_url,
  cast(null as string) as interaction_content_title,
  reactionType as interaction_event_name,
  cast(null as string) as interaction_event_details,
  `timestamp` as interaction_event_ts
FROM
  interactions c
)
select
  *
from
  renamed
where
  contact_id is not null
and
  interaction_content_id is not null


  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
