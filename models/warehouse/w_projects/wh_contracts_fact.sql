{% if var("projects_warehouse_contract_sources") %}
{{
    config(
        unique_key='contract_pk',
        alias='contracts_fact'
    )
}}


WITH contacts_dim as (
  SELECT
    contact_pk,
    all_contact_ids as contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) as all_contact_ids
  ),
contracts AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_contracts')) }}
  FROM   {{ ref('int_contracts') }}
),
email_domains as (
  select *
  from {{ ref('int_email_domains')}}
),
contract_companies as (
  SELECT {{ dbt_utils.star(from=ref('wh_contact_companies_fact')) }}
  FROM   {{ ref('wh_contact_companies_fact') }}
),companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
)
SELECT
{{ dbt_utils.surrogate_key(['contract_id']) }} as contract_pk,
   t.* ,
   c.contact_pk,
   cd.company_pk,
   e.email_domain
FROM
   contracts t
left join   contacts_dim c
   on     t.contact_id = c.contact_id
left join email_domains e
   on split(contact_email,'@')[safe_offset(1)] = e.email_domain
JOIN companies_dim cd
        ON e.contact_company_id IN UNNEST(cd.all_company_ids)
{% else %}

{{config(enabled=false)}}

{% endif %}
