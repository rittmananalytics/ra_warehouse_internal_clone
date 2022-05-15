{% if var('marketing_warehouse_meetings_sources') %}



with meetings_merge_list as

  (
    {{ merge_sources(sources=var('marketing_warehouse_meetings_sources'),model_suffix='_meetings') }}
  )

select
  *
from
  meetings_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
