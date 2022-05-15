{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'github_starrers' in var("crm_warehouse_contact_sources") %}

WITH contacts AS (
  select
    *
  from
    {{ source('github_starrers','contacts' ) }} p
),
renamed as (
  SELECT
  concat('{{ var('stg_github_starrers_id-prefix') }}',username) contact_id,
  SPLIT(fullname)[safe_OFFSET(0)] contact_first_name,
  SPLIT(fullname)[safe_OFFSET(1)] contact_last_name,
  fullname contact_name,
  CAST(NULL AS string) contact_job_title,
  email contact_email,
  CAST(NULL AS string) contact_phone,
  location contact_address,
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
  bio AS contact_bio,
  CAST(NULL AS int64) AS contact_friends_count,
  CAST(NULL AS int64) AS contact_posts_count,
  true contact_is_following,
  false contact_is_followed_by_us,
  CAST(NULL AS boolean) contact_is_active,
  cast(null as timestamp) contact_created_date,
  Row_Updated_At contact_last_modified_date
FROM
  contacts
WHERE username is not null
{{ dbt_utils.group_by(30) }}
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
