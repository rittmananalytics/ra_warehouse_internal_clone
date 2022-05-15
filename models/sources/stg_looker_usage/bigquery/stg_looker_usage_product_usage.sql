{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'looker_usage' in var("product_warehouse_usage_sources") %}

with source as (
    SELECT
      *
    FROM
      {{ ref('stg_looker_usage_stats') }}
  ),
 renamed as
 (
  SELECT
        company_id                                                              as product_account_id,
        concat('looker-',
             case when title is not null then 'look'
             when dashboard_title is not null then 'dashboard'
             else 'explore' end)           as product_id,
        concat('looker-','N/A')                                                    as product_sku_id,
        company_id                                                              as company_id,
        cast (null as string)                                                   as product_project_id,
        timestamp(created_time)                                                 as product_usage_billing_ts,
        timestamp(created_time)                                                 as product_usage_start_ts,
        cast(null as timestamp)                                                 as product_usage_end_ts,
        cast (null as string)                                                   as product_usage_location,
        cast (null as string)                                                   as product_usage_country,
        cast (null as string)                                                   as product_usage_region,
        cast (null as string)                                                   as product_usage_zone,
        'Seconds'                                                               as product_usage_unit,
        cast (null as string)                                                   as product_usage_currency,
        average_runtime_in_seconds                                              as product_usage_cost,
        approximate_web_usage_in_minutes/60                                     AS product_usage_amount,
        cast (null as float64)                                                  as product_currency_conversion_rate,
        null                                                                    as product_usage_row_count,
        case when title is not null then title
        when dashboard_title is not null then dashboard_title
        else null end                                                           as product_usage_query_text,
        md5(lower(replace(
            case when title is not null then title
            when dashboard_title is not null then dashboard_title
            else null end,' ','')
          )) as product_usage_query_hash,
        cast(null as string)                                                    as product_usage_priority,
        status                                                                  as product_usage_status,
        cast(null as string)                                                    as product_usage_error_code,
        cast(null as string)                                                    as product_usage_error_status,
        cast(id as string)                                                                as product_usage_job_id,
        contact_id                                                              as contact_id
        FROM
          source
        )
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
