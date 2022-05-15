{% if var("marketing_warehouse_email_event_sources")  %}

{{
    config(
        alias='email_lists_dim'
    )
}}


WITH lists AS
  (
  SELECT * from {{ ref('int_email_lists') }}
)
select {{ dbt_utils.surrogate_key(['list_id']) }} as list_pk,
       l.*
from lists l


{% else %} {{config(enabled=false)}} {% endif %}
