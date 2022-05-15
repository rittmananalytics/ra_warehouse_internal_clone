{% if var('crm_warehouse_contact_sources') %}

{{config(materialized="table")}}

with t_contacts_merge_list as
  (
      {% set relations_list = [] %}
      {% for source in var('crm_warehouse_contact_sources') %}
        {% do relations_list.append(ref('stg_' ~ source ~ '_contacts')) %}
      {% endfor %}

      {{ dbt_utils.union_relations(
        relations=relations_list)
      }}
    )

SELECT
  * except (contact_name),
  case when instr(contact_name,' ') > 0 then
      concat(
          initcap(
            REGEXP_REPLACE(
            split(contact_name,' ')[safe_offset(0)]
            , r'\W', '')
            )
          ,' ',
          initcap(
            REGEXP_REPLACE(
          split(contact_name,' ')[safe_offset(1)]
          , r'\W', '')
            )
          )
        else contact_name end
           as contact_name
FROM
  t_contacts_merge_list
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
