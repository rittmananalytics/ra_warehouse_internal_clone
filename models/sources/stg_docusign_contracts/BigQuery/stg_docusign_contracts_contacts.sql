{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_contract_sources") %}
{% if 'docusign_contracts' in var("crm_warehouse_contact_sources") %}


WITH source AS (
  SELECT
    contact_id,
    SPLIT(contact_name,' ')[safe_OFFSET(0)] AS contact_first_name,
    SPLIT(contact_name,' ')[safe_OFFSET(1)] AS contact_last_name,
    contact_name,
    max(contact_title) AS job_title,
    contact_email,
    CAST(NULL AS STRING) AS contact_phone,
    CAST(NULL AS STRING) AS contact_address,
    CAST(NULL AS STRING) AS contact_city,
    CAST(NULL AS STRING) AS contact_state,
    CAST(NULL AS STRING) AS contact_country,
    CAST(NULL AS STRING) AS contact_postcode_zip,
    CAST(NULL AS STRING) AS contact_company,
    CAST(NULL AS STRING) AS contact_website,
    CAST(NULL AS STRING) AS contact_company_id,
    CAST(NULL AS STRING) AS contact_owner_id,
    CAST(NULL AS STRING) AS contact_lifecycle_stage,
    CAST(NULL AS boolean) AS contact_is_contractor,
    CASE
      WHEN contact_email LIKE '%@rittmananalytics%' THEN TRUE
    ELSE
    FALSE
  END
    AS contact_is_staff,
    CAST(NULL AS int64) AS contact_weekly_capacity,
    CAST(NULL AS int64) AS contact_default_hourly_rate,
    CAST(NULL AS int64) AS contact_cost_rate,
    CAST(NULL AS STRING) AS contact_bio,
    cast(null as int64) AS contact_friends_count,
    cast(null as int64) AS contact_posts_count,
    cast(null as boolean)  contact_is_following,
    cast(null as boolean)  contact_is_followed_by_us,
    CAST(NULL AS boolean) AS contact_is_active,
    CAST(NULL AS timestamp) AS contact_created_date,
    CAST(NULL AS timestamp) AS contact_last_modified_date
  FROM
    {{ ref('stg_docusign_contracts_contracts') }}
  group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
      )
SELECT
 *
FROM
 source

       {% else %} {{config(enabled=false)}} {% endif %}
       {% else %} {{config(enabled=false)}} {% endif %}
