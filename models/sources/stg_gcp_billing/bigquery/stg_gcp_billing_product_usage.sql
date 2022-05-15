{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'gcp_billing' in var("product_warehouse_usage_sources") %}

with source as (
    SELECT
      *
    FROM
      {{ source('gcp_billing', 'gcp_billing_export') }}
  ),
 renamed as
 (
  SELECT
        billing_account_id                                                          as product_account_id,
        coalesce(concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',service.id),'N/A')   as product_id,
        coalesce(concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',sku.id),'N/A')       as product_sku_id,
        concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',project.id)   as company_id,
        project.id                                as product_project_id,
        parse_timestamp('%Y%m',invoice.month)     as product_usage_billing_ts,
        usage_start_time                          as product_usage_start_ts,
        usage_end_time                            as product_usage_end_ts,
        location.location                         as product_usage_location,
        location.country                          as product_usage_country,
        location.region                           as product_usage_region,
        location.zone                             as product_usage_zone,
        usage.unit                                as product_usage_unit,
        currency                                  as product_usage_currency,
        cost                                      as product_usage_cost,
        usage.amount                              AS product_usage_amount,
        currency_conversion_rate                  as product_currency_conversion_rate,
        null                                      as product_usage_row_count,
        cast(null as string) as product_usage_query_text,
        md5(cast(null as string)) as product_usage_query_hash,
        cast(null as string) as product_usage_priority,
        cast(null as string) as product_usage_status,
        cast(null as string) as product_usage_error_code,
        cast(null as string) as product_usage_error_status,
        cast(null as string) as product_usage_job_id,
        cast(null as string) as contact_id
        FROM
          source
        )
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
