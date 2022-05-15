{% if var("finance_warehouse_payment_sources") %}

{{
    config(
        unique_key='payment_pk',
        alias='payments_fact'
    )
}}


WITH payments AS
  (
  SELECT *
  FROM   {{ ref('int_payments') }}
  ),
  companies_dim as (
      select *
      from {{ ref('wh_companies_dim') }}
  ),
  currencies_dim as (
    select *
    from {{ ref('wh_currency_dim') }}
),
payments_fact as(
SELECT
   {{ dbt_utils.surrogate_key(['payment_id']) }} as payment_pk,
   p.*
FROM
   payments p),
invoice_payments as (SELECT * FROM (
   SELECT invoice_pk, invoice_id
   FROM {{ ref('wh_invoices_fact')}} i,
   UNNEST(all_invoice_ids) invoice_id
   )
)
select p.*,i.invoice_pk as payment_invoice_fk
from payments_fact p
left join invoice_payments i
on p.invoice_id = i.invoice_id

{% else %} {{config(enabled=false)}} {% endif %}
