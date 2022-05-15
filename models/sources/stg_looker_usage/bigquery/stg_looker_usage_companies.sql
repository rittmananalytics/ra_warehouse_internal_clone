{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'looker_usage' in var("crm_warehouse_company_sources") %}

WITH source AS (

    select
      *
    from
      `ra-development.fivetran_email.usage_stats`

),
renamed as (
select * from (
SELECT
    concat('looker-',client) AS company_id,
    client AS company_name,
    cast (null as string) as company_address,
    cast (null as string) AS company_address2,
    cast (null as string) AS company_city,
    cast (null as string) AS company_state,
    cast (null as string) AS company_country,
    cast (null as string) AS company_zip,
    cast (null as string) AS company_phone,
    cast (null as string) AS company_website,
    cast (null as string) AS company_industry,
    cast (null as string) AS company_linkedin_company_page,
    cast (null as string) AS company_linkedin_bio,
    cast (null as string) AS company_twitterhandle,
    cast (null as string) AS company_description,
    cast (null as string) as company_finance_status,
    cast (null as string)     as company_currency_code,
    cast (null as string)  as company_lifecycle_stage,
    cast (null as timestamp) as company_lifecycle_stage_ts,
    min(timestamp(history_created_time)) over (partition by client) as company_created_date,
    max(timestamp(history_created_time)) over (partition by client) as company_last_modified_date
    FROM source )
    {{ dbt_utils.group_by(n=19) }} )
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
