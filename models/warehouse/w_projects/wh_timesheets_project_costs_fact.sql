{% if var("projects_warehouse_timesheet_sources") %}
  {{ config(
    unique_key = 'expense_pk',
    alias = 'timesheet_project_costs_fact'
  ) }}

  WITH {% if target.type == 'bigquery' %}
    companies_dim AS (

      SELECT
        {{ dbt_utils.star(
          from = ref('wh_companies_dim')
        ) }}
      FROM
        {{ ref('wh_companies_dim') }}
    ),
    contacts_dim AS (
      SELECT
        {{ dbt_utils.star(
          from = ref('wh_contacts_dim')
        ) }}
      FROM
        {{ ref('wh_contacts_dim') }}
    ),
    {% elif target.type == 'snowflake' %}
    companies_dim AS (
      SELECT
        C.company_pk,
        cf.value :: STRING AS company_id
      FROM
        {{ ref('wh_companies_dim') }} C,
        TABLE(FLATTEN(C.all_company_ids)) cf
    ),
contacts_dim as (
    SELECT c.contact_pk, cf.value::string as contact_id
    from {{ ref('wh_contacts_dim') }} c,table(flatten(c.all_contact_ids)) cf),

  {% else %}
    {{ exceptions.raise_compiler_error(
      target.type ~ " not supported in this project"
    ) }}
  {% endif %}
  projects_dim AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('wh_timesheet_projects_dim')
      ) }}
    FROM
      {{ ref('wh_timesheet_projects_dim') }}
  ),
  invoices AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('wh_invoices_fact')
      ) }}
    FROM
      {{ ref('wh_invoices_fact') }}
  )
  ,
  expenses AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('int_timesheet_project_costs')
      ) }}
    FROM
      {{ ref('int_timesheet_project_costs') }}
  )
SELECT
{{ dbt_utils.surrogate_key(['expense_id']) }} as expense_pk,
  C.company_pk,
  u.contact_pk,
  p.timesheet_project_pk,
  t.*
FROM
  expenses t

  {% if target.type == 'bigquery' %}
  JOIN companies_dim C
  ON t.company_id IN unnest(
    C.all_company_ids
  )
  LEFT JOIN contacts_dim u
  ON CAST(
    t.timesheet_users_id AS STRING
  ) IN unnest(
    u.all_contact_ids
  )
  {% elif target.type == 'snowflake' %}
  JOIN companies_dim C
  ON t.company_id = C.company_id
  JOIN contacts_dim u
  ON t.timesheet_users_id :: STRING = u.contact_id
{% else %}
  {{ exceptions.raise_compiler_error(
    target.type ~ " not supported in this project"
  ) }}
{% endif %}
LEFT OUTER JOIN projects_dim p
ON t.timesheet_project_id = p.timesheet_project_id
{% else %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
