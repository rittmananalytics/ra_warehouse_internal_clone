{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'harvest_forecast' in var("crm_warehouse_contact_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_harvest_forecast_contacts_table'),unique_column='harvest_user_id') }}
),
renamed as (
select
    concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(harvest_user_id as string)) as contact_id,
    first_name as contact_first_name,
    last_name as contact_last_name,
    concat(first_name,' ',last_name) as contact_name,
    cast(null as string) as contact_job_title,
    email as contact_email,
    cast(null as string) as contact_phone,
    cast(null as string) as contact_address,
    cast(null as string) as contact_city,
    cast(null as string) as contact_state,
    cast(null as string) as contact_country,
    cast(null as string) as contact_postcode_zip,
    'rittman analytics' as contact_company,
    'rittmananalytics.com' as contact_website,
    cast(null as string) as contact_company_id,
    cast(null as string) as contact_owner_id,
    cast(null as string) as contact_lifecycle_stage,
    cast(null as boolean) as contact_is_contractor,
    true as contact_is_staff,
    weekly_capacity as contact_weekly_capacity,
    cast(null as int64) as contact_default_hourly_rate,
    cast(null as int64) as contact_cost_rate,
    CAST(NULL AS STRING) AS contact_bio,
    cast(null as int64) AS contact_friends_count,
    cast(null as int64) AS contact_posts_count,
    cast(null as boolean)  contact_is_following,
    cast(null as boolean)  contact_is_followed_by_us,
    cast(null as boolean) as contact_is_active,
    min(updated_at) over (partition by harvest_user_id) as contact_created_date,
    updated_at as contact_last_modified_date
from source)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
