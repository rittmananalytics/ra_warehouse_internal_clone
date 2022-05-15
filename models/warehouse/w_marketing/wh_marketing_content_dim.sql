{% if var("marketing_warehouse_interaction_content_sources")  %}

{{
    config(
        alias='marketing_content_dim'
    )
}}


WITH lists AS
  (
  SELECT * from {{ ref('int_interaction_content') }}
)
select {{ dbt_utils.surrogate_key(['interaction_content_id']) }} as marketing_content_pk,
       l.*
from lists l


{% else %} {{config(enabled=false)}} {% endif %}
