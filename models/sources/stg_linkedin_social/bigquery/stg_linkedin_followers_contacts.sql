{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'linkedin_followers' in var("crm_warehouse_contact_sources") %}

WITH contacts AS (
  select
    *
  from
    {{ source('linkedin_followers','contacts' ) }}
),
  profiles AS (
    select
      *
    from
      {{ source('linkedin_followers','profiles' ) }}
  ),
renamed as (
SELECT
  concat('{{ var('stg_linkedin_social_id-prefix') }}',concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', ''))) as contact_id,
  REGEXP_REPLACE(c.firstName, r'\W', '') contact_first_name,
  REGEXP_REPLACE(c.lastName, r'\W', '') contact_last_name,
  concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) contact_name,
  cast(coalesce(p.jobTitle,c.headline) as string) contact_job_title,
  CAST(p.email AS string) contact_email,
  CAST(NULL AS string) contact_phone,
  coalesce(location,CAST(NULL AS string)) contact_address,
  CAST(NULL AS string) contact_city,
  CAST(NULL AS string) contact_state,
  CAST(NULL AS string) contact_country,
  CAST(NULL AS string) contact_postcode_zip,
  coalesce(p.company,CAST(NULL AS string)) contact_company,
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
  true contact_is_following,
  false contact_is_followed_by_us,
  CAST(NULL AS boolean) contact_is_active,
  CAST(p.jobDescription AS string) contact_job_description,
  CAST(p.school AS string) contact_school,
  CAST(p.description AS string) contact_description,
  CAST(p.subscribers AS int64) contact_subscribers,
  CAST(p.connectionDegree AS string) contact_connection_degree,
  CAST(p.connectionsCount AS int64) contact_connections_count,
  CAST(p.mutualConnectionsText AS string) contact_mutual_connections,
  CAST(p.mailFromDropcontact AS string) contact_mail_from_drop_contact,
  CAST(p.schoolDegree AS string) contact_school_degree,
  CAST(p.schoolDescription AS string) contact_school_description,
  CAST(p.qualificationFromDropContact AS string) contact_qualifications,
  CAST(p.allSkills AS string) contact_skills,
  parse_timestamp('%b %Y',c.followedAt)  contact_created_date,
  CAST(c.Row_Updated_At as timestamp) contact_last_modified_date
FROM
  contacts c
left join
  profiles p
on
  concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) = p.fullname
WHERE concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) is not null
{{ dbt_utils.group_by(42) }}
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
