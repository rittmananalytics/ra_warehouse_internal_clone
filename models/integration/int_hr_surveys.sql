{% if var('hr_warehouse_survey_sources') %}



with hr_survey_merge_list as
  (
    {% for source in var('hr_warehouse_survey_sources') %}
      {% set relation_source = 'stg_' + source + '_survey_results' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from hr_survey_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
