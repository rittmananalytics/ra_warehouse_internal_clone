{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'looker_usage' in var("product_warehouse_usage_sources") %}


with source as (
    SELECT
      concat('looker-','N/A')                                                     as product_sku_id,
      'N/A'                                                            as product_sku_name,
      concat('looker-',
           case when title is not null then 'look'
           when dashboard_title is not null then 'dashboard'
           else 'explore' end)           as product_id,
           case when title is not null then 'look'
                when dashboard_title is not null then 'dashboard'
                else 'explore' end  as product_name,
       concat('looker-looker_query') as product_category_id,
       'looker_query' as product_category_name
FROM `ra-development.analytics_staging.stg_looker_usage_stats`
group by 1,2,3,4,5,6)
select * from source

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
