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

select split(contact_email,'@')[safe_offset(1)] as email_domain,
       contact_company_id
from   t_contacts_merge_list
where contact_company_id is not null
and   split(contact_email,'@')[safe_offset(1)] is not null
group by 1,2

{% endif %}
