{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_meetings_sources") %}
{% if 'calendly_discovery' in var("marketing_warehouse_meetings_sources") %}

WITH meetings AS (
  select
    *
  from
    {{ source('calendly_meetings','discovery' ) }}
  )
,
renamed as (
SELECT
  concat('{{ var('stg_calendly_meetings_id-prefix') }}',id) as meeting_id,
  concat('{{ var('stg_calendly_meetings_id-prefix') }}',full_name) as contact_id,
  full_name as contact_name,
  email as contact_email,
  event as meeting_event_name,
  meeting_purpose as meeting_purpose,
  timestamp as meeting_ts,
FROM
  meetings
)
select
  *
from
  renamed c


  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
