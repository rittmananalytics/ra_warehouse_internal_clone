{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_nps' in var("crm_warehouse_contact_sources") %}

with source as (
  select contact_name,
         email,
         survey_type,
         survey_name,
         rating,
         sentiment,
         response_content,
         submission_date
  from   {{ ref('hubspot_nps_responses') }}
),
renamed as (
  SELECT
  concat('{{ var('stg_hubspot_nps_id-prefix') }}',cast(contact_name as string)) as contact_id,
  split(contact_name,' ')[safe_offset(0)] as contact_first_name,
  split(contact_name,' ')[safe_offset(1)] as contact_last_name,
  contact_name as contact_name,
  cast(null as string) as contact_job_title,
  email as contact_email,
  cast(null as string)  as contact_phone,
  cast(null as string)  as contact_address,
  cast(null as string)  as contact_city,
  cast(null as string)  as contact_state,
  cast(null as string)  as contact_country,
  cast(null as string)  as contact_postcode_zip,
  cast(null as string)  as contact_company,
  cast(null as string)  as contact_website,
  cast(null as string) as contact_company_id,
  cast(null as string)  as contact_owner_id,
  cast(null as string)  as contact_lifecycle_stage,
  false                              as contact_is_contractor,
  false                               as contact_is_staff,
  null                              as contact_weekly_capacity,
  null                              as contact_default_hourly_rate,
  null                              as contact_cost_rate,
  CAST(NULL AS STRING) AS contact_bio,
  cast(null as int64) AS contact_friends_count,
  cast(null as int64) AS contact_posts_count,
  cast(null as boolean)  contact_is_following,
  cast(null as boolean)  contact_is_followed_by_us,
  true                          as contact_is_active,
  cast(null as timestamp)  as contact_created_date,
  parse_timestamp('%Y-%m-%d %H:%M',submission_date) as contact_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
