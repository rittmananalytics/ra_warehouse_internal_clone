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
         concat('{{ var('stg_gcp_billing_product_usage_id-prefix') }}',project.id)  as company_id,
         project.id as company_name,
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
         cast (null as timestamp) as company_created_date,
         cast (null as timestamp) as company_last_modified_date
  FROM source
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
