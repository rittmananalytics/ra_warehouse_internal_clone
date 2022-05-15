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
    coalesce(concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',sku.id),'N/A')          as product_sku_id,
    sku.description                                                                as product_sku_name,
    coalesce(concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',service.id),'N/A')      as product_id,
    service.description                                                            as product_name,
    concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}','gcp')           as product_source_id,
    'GCP Billing Export'                                                           as product_source_name
FROM source
    group by 1,2,3,4,5,6)
SELECT
    *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
