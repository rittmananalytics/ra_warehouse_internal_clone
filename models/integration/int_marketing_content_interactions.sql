{% if var('marketing_warehouse_content_interaction_sources') %}

{{config(materialized="view")}}

with t_interaction_sources as
  (
      {% set relations_list = [] %}
      {% for source in var('marketing_warehouse_content_interaction_sources') %}
        {% do relations_list.append(ref('stg_' ~ source ~ '_content_interactions')) %}
      {% endfor %}

      {{ dbt_utils.union_relations(
        relations=relations_list)
      }}
    )


SELECT
  * except (interaction_content_url, interaction_event_name),
  split(interaction_content_url,'?')[safe_offset(0)] as interaction_content_url,
  case when interaction_event_name = 'liked' then 'like' else interaction_event_name end as interaction_event_name
FROM
  t_interaction_sources
where
  contact_id is not null
and not
  (interaction_content_id	is null and interaction_event_name != 'follow')
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
