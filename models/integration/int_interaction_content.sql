{% if var('marketing_warehouse_interaction_content_sources') %}

{{config(materialized="view")}}

with t_content_sources as
  (
      {% set relations_list = [] %}
      {% for source in var('marketing_warehouse_interaction_content_sources') %}
        {% do relations_list.append(ref('stg_' ~ source ~ '_interaction_content')) %}
      {% endfor %}

      {{ dbt_utils.union_relations(
        relations=relations_list)
      }}
    )


    SELECT
      *
    FROM (
      SELECT
        interaction_content_id,
        min(interaction_posted_ts) over (partition by interaction_content_id) interaction_posted_ts,
        interaction_content,
        utm_source,
        utm_medium,
        utm_account,
        max(interaction_reported_like_count) over (partition by interaction_content_id) interaction_reported_like_count,
        max(interaction_reported_comment_count) over (partition by interaction_content_id) interaction_reported_comment_count,
        max(post_reported_follower_count) over (partition by interaction_content_id) post_reported_follower_count
      FROM
        t_content_sources
    )
    group by 1,2,3,4,5,6,7,8,9
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
