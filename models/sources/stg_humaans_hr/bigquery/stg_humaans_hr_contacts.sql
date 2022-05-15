{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'humaans_hr' in var("crm_warehouse_contact_sources") %}

WITH people AS (
  select
    *
  from
    {{ source('humaans_hr','contacts' ) }} p
),
  roles as (
    select
      *
    from
      {{ source('humaans_hr','roles' ) }} p
),
joined_and_renamed AS (
  select
    concat('{{ var('stg_humaans_hr_id-prefix') }}',p.data_id) as contact_id,
    replace(replace(data_firstname,'Tobias','Toby'),'Zachy','Amir') as contact_first_name,
    data_lastname as contact_last_name,
    concat(replace(replace(data_firstname,'Tobias','Toby'),'Zachy','Amir'),' ',data_lastname) as contact_name,
    data_jobtitle as contact_job_title,
    data_email as contact_email,
    data_personalphonenumber as contact_phone,
    data_address as contact_address,
    data_city as contact_city,
    data_state as contact_state,
    data_countrycode as contact_country,
    data_postcode as contact_postcode_zip,
    'rittman analytics' as contact_company,
    'rittmananalytics.com' as contact_website,
    cast(null as string) as contact_company_id,
    cast(null as string) as contact_owner_id,
    cast(null as string) as contact_lifecycle_stage,
    cast(null as boolean) as contact_is_contractor,
    true as contact_is_staff,
    cast(null as int64) as contact_weekly_capacity,
    cast(null as int64) as contact_default_hourly_rate,
    cast(null as int64) as contact_cost_rate,
    CAST(NULL AS STRING) AS contact_bio,
    cast(null as int64) AS contact_friends_count,
    cast(null as int64) AS contact_posts_count,
    cast(null as boolean)  contact_is_following,
    cast(null as boolean)  contact_is_followed_by_us,
    data_status='active' as contact_is_active,
    p.data_createdat as contact_created_date,
    p.data_updatedat as contact_last_modified_date
  from
    people p
  left join
    roles j
  on
    p.data_id = j.data_personid
  where
    j.data_enddate is null
)
select
  *
from
  joined_and_renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
