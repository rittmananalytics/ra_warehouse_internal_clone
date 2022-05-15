{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_nps_sources") %}
{% if 'hubspot_nps' in var("crm_warehouse_nps_sources") %}

with source as (
  select
  contact_name,
  survey_type,
  survey_name,
  rating,
  sentiment,
  response_content,
  submission_date
from
  {{ ref('hubspot_nps_responses') }}
),
renamed as (
  select
    concat('{{ var('stg_hubspot_nps_id-prefix') }}',cast(contact_name as string)) as contact_id,
    concat('{{ var('stg_hubspot_nps_id-prefix') }}',cast(survey_name as string)) as nps_survey_id,
    survey_name as nps_survey__name,
    survey_type as nps_survey_type,
    rating as nps_survey_response_score,
    initcap(sentiment) as nps_survey_response_sentiment,
    response_content as nps_survey_response_content,
    parse_timestamp('%Y-%m-%d %H:%M',submission_date) as nps_survey_response_created_at_ts
from source
)
select *
from   renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
