{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'calendly_discovery' in var("crm_warehouse_contact_sources") %}

WITH contacts AS (
  select
    *
  from
  (
    SELECT
      full_name,
      email,
      min(timestamp) over (partition by email) as timestamp
    FROM
      {{ source('calendly_meetings','discovery' ) }}
  )
  group by 1,2,3
),
renamed as (
SELECT
  concat('{{ var('stg_calendly_meetings_id-prefix') }}',full_name) as contact_id,
  split(full_name)[safe_offset(0)] as contact_first_name,
  split(full_name)[safe_offset(1)] as contact_last_name,
  full_name as contact_name,
  CAST(NULL AS string) contact_job_title,
  email as contact_email,
  CAST(NULL AS string) contact_phone,
  CAST(NULL AS string) contact_address,
  CAST(NULL AS string) contact_city,
  CAST(NULL AS string) contact_state,
  CAST(NULL AS string) contact_country,
  CAST(NULL AS string) contact_postcode_zip,
  CAST(NULL AS string) contact_company,
  CAST(NULL AS string) contact_website,
  CAST(NULL AS string) contact_company_id,
  CAST(NULL AS string) contact_owner_id,
  CAST(NULL AS string) contact_lifecycle_stage,
  CAST(NULL AS boolean) contact_is_contractor,
  CAST(NULL AS boolean) contact_is_staff,
  CAST(NULL AS int64) contact_weekly_capacity,
  CAST(NULL AS int64) contact_default_hourly_rate,
  CAST(NULL AS int64) contact_cost_rate,
  CAST(NULL AS string) AS contact_bio,
  CAST(NULL AS int64) AS contact_friends_count,
  CAST(NULL AS int64) AS contact_posts_count,
  false contact_is_following,
  false contact_is_followed_by_us,
  CAST(NULL AS boolean) contact_is_active,
  CAST(NULL AS string) contact_job_description,
  CAST(NULL AS string) contact_school,
  CAST(NULL AS string) contact_description,
  CAST(NULL AS string) contact_subscribers,
  CAST(NULL AS string) contact_connection_degree,
  CAST(NULL AS int64) contact_connections_count,
  CAST(NULL AS string)  contact_mutual_connections,
  CAST(NULL AS string)  contact_mail_from_drop_contact,
  CAST(NULL AS string)  contact_school_degree,
  CAST(NULL AS string)  contact_school_description,
  CAST(NULL AS string)  contact_qualifications,
  CAST(NULL AS string)  contact_skills,
  `timestamp` as  contact_created_date,
  CAST(null as timestamp) contact_last_modified_date
FROM
  contacts c
{{ dbt_utils.group_by(42) }}
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
