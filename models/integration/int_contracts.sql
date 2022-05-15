{% if var('projects_warehouse_contract_sources') %}

WITH contracts_merge_list AS (

  {% for source in var('projects_warehouse_contract_sources') %}
    {% set relation_source = 'stg_' + source + '_contracts' %}

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
  contracts_merge_list

{% else %}

  {{config(enabled=false)}}

{% endif %}
