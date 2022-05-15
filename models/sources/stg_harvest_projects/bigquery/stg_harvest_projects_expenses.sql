{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_expenses_table'),unique_column='id') }}
),
expense_categories as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_expense_categories_table'),unique_column='id') }}
),
clients as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_companies_table'),unique_column='id') }}
),
joined as (
  select
    concat('{{ var('stg_harvest_projects_id-prefix') }}',s.id) as expense_id,
    cast(s.project_id as string) as timesheet_project_id,
    concat('{{ var('stg_harvest_projects_id-prefix') }}',s.client_id) as company_id,
    concat('{{ var('stg_harvest_projects_id-prefix') }}',s.invoice_id) as invoice_id,
    s.created_at as expense_created_at_ts,
    s.is_billed as expense_is_billed,
    s.is_locked as expense_is_locked,
    s.billable as expense_is_billable,
    s.updated_at as expense_updated_at_ts,
    timestamp(s.spent_date) as expense_spent_at_ts,
    s.units as expense_units,
    e.unit_name as expense_unit_name,
    e.unit_price as expense_unit_price,
    e.name as expense_category_name,
    s.total_cost as expense_amount_local,
    c.currency as expense_currency_code,
    concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(s.user_id as string))  as timesheet_users_id,
    s.notes as expense_notes,
    s.expense_category_id as expense_category_id
  from
    source s
  join
    clients c
  on
    s.client_id = c.id
  left join
    expense_categories e
  on
    s.expense_category_id = e.id
  )
select
  *
from
  joined

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
