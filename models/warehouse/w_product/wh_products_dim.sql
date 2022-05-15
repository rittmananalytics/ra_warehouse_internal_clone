{% if var("product_warehouse_usage_sources")  %}

{{
    config(
        unique_key='product_pk',
        alias='products_dim'
    )
}}


WITH products AS
  (
  SELECT * from {{ ref('int_products') }}
)
select {{ dbt_utils.surrogate_key(['product_id']) }} as product_pk,
       a.*
from products a

{% else %} {{config(enabled=false)}} {% endif %}
