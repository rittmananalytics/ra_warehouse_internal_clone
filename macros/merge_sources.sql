{%- macro merge_sources(sources, model_suffix) -%}
(
    {% set relations_list = [] %}
    {% for source in sources %}
      {% do relations_list.append(ref('stg_' ~ source ~ model_suffix)) %}
    {% endfor %}

    {{ dbt_utils.union_relations(
      relations=relations_list)
    }}
  )

{%- endmacro -%}
