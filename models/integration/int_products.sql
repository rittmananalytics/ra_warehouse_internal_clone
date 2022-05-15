{% if var('product_warehouse_usage_sources') %}

WITH t_product_merge_list AS (

  {% for source in var('product_warehouse_usage_sources') %}
    {% set relation_source = 'stg_' + source + '_products' %}

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
  t_product_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
