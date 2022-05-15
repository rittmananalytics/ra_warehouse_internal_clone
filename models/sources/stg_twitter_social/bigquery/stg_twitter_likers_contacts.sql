{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'twitter_likers' in var("crm_warehouse_contact_sources") %}

WITH contacts AS (
  select
    *
  from
    {{ source('twitter_tweets','contacts' ) }} p
),
renamed as (
SELECT
  concat('{{ var('stg_twitter_social_id-prefix') }}',replace(handle,'@','')) contact_id,
  SPLIT(name)[safe_OFFSET(0)] contact_first_name,
  SPLIT(name)[safe_OFFSET(1)] contact_last_name,
  name contact_name,
  CAST(NULL AS string) contact_job_title,
  CAST(NULL AS string) contact_email,
  CAST(NULL AS string) contact_phone,
  location contact_address,
  CAST(NULL AS string) contact_city,
  CAST(NULL AS string) contact_state,
  CAST(NULL AS string) contact_country,
  CAST(NULL AS string) contact_postcode_zip,
  CAST(NULL AS string) contact_company,
  website contact_website,
  CAST(NULL AS string) contact_company_id,
  CAST(NULL AS string) contact_owner_id,
  CAST(NULL AS string) contact_lifecycle_stage,
  CAST(NULL AS boolean) contact_is_contractor,
  CAST(NULL AS boolean) contact_is_staff,
  CAST(NULL AS int64) contact_weekly_capacity,
  CAST(NULL AS int64) contact_default_hourly_rate,
  CAST(NULL AS int64)contact_cost_rate,
  bio AS contact_bio,
  followers AS contact_friends_count,
  tweetscount AS contact_posts_count,
  cast(null as boolean) contact_is_following,
  cast(null as boolean) contact_is_followed_by_us,
  CAST(NULL AS boolean) contact_is_active,
  joindate contact_created_date,
  cast(Row_Updated_At as timestamp) contact_last_modified_date
FROM
  contacts
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
