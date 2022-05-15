{% if var("product_warehouse_usage_sources")  %}

{{
    config(
        alias='product_usage_fact'
    )
}}


with companies_dim as (
    select company_pk, company_id
    from {{ ref('wh_companies_dim') }},
    unnest(all_company_ids) as company_id
),contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
products_dim as (
  select *
  from {{ ref('wh_products_dim') }}
)
SELECT
   GENERATE_UUID() as product_usage_pk,
--   c.company_pk,
   p.product_pk,
   c.company_pk,
   e.contact_pk,
   d.*
FROM
   {{ ref('int_product_usage') }} d
LEFT OUTER JOIN
     products_dim p
   ON d.product_id = p.product_id
LEFT OUTER JOIN companies_dim c
   ON d.company_id = c.company_id
LEFT OUTER JOIN contacts_dim e
   ON d.contact_id = e.contact_id
   {% else %} {{config(enabled=false)}} {% endif %}
