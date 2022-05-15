{% if var("projects_warehouse_timesheet_sources") %}

with t_costs_merge_list as
  (
    {% for source in var('projects_warehouse_timesheet_sources') %}
      {% set relation_source = 'stg_' + source + '_expenses' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_costs_merge_list
where expense_is_billable is not true


{% else %} {{config(enabled=false)}} {% endif %}
