{% if var('product_warehouse_usage_sources') %}

WITH t_product_usage_merge_list AS (

  {% for source in var('product_warehouse_usage_sources') %}
    {% set relation_source = 'stg_' + source + '_product_usage' %}

    select
      '{{source}}' as source,
      *
      from {{ ref(relation_source) }}

      {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
SELECT
  *
FROM
  t_product_usage_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
