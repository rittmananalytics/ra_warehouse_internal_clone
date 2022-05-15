{% if var('crm_warehouse_nps_sources') %}

WITH t_nps_survey_responses_list AS (

  {% for source in var('crm_warehouse_nps_sources') %}
    {% set relation_source = 'stg_' + source + '_survey_responses' %}

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
  t_nps_survey_responses_list

{% else %} {{config(enabled=false)}} {% endif %}
