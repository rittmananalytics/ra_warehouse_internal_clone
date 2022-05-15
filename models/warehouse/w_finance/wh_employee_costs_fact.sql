{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        unique_key='employee_costs_fact_pk',
        alias='employee_costs_fact'
    )
}}

SELECT
    {{ dbt_utils.surrogate_key(['journal_month','Employee_Name','general_ledger_fact_reference']) }} as employee_costs_fact_pk,
    * FROM (SELECT
    date_trunc(date(general_ledger_fact.journal_date),MONTH) as journal_month,
    case when general_ledger_fact.description like '%Baker%' then 'Lewis Baker'
         when general_ledger_fact.description like '%Sanderson%' then 'George Sanderson'
         when general_ledger_fact.description like '%J Rittman%' then 'Janet Rittman'
         when general_ledger_fact.description like '%Calleja%' then 'Mike Calleja'
         when general_ledger_fact.description like '%Bramwell%' then 'Rob Bramwell'
         when general_ledger_fact.description like '%Kane%' then 'David Kane'
         when general_ledger_fact.description like '%Berrystone%' or general_ledger_fact.description like '%Bwerrystone%' then 'Will Berrystone'
         when general_ledger_fact.description like '%Mark Rittman%' then 'Mark Rittman'
         else null end as Employee_Name,
    general_ledger_fact.reference  AS general_ledger_fact_reference,
    case when chart_of_accounts_dim.account_name in ('Direct Wages','Sales Wages','Directors Renumeration','Trainee Wages') then 'Salary'
         when chart_of_accounts_dim.account_name in ('Employers National Insurance','Pensions Costs') then 'Employers NIC & Pension'
         when chart_of_accounts_dim.account_name like '%Bonus%' then 'Bonus' end as Employment_Cost_Type,
    COALESCE(SUM(coalesce(general_ledger_fact.net_amount * -1,0)), 0) AS general_ledger_fact_net_amount
FROM {{ ref('wh_chart_of_accounts_dim') }}
     AS chart_of_accounts_dim
LEFT JOIN {{ ref('wh_general_ledger_fact') }}
     AS general_ledger_fact ON chart_of_accounts_dim.account_id = general_ledger_fact.account_id
WHERE ((chart_of_accounts_dim.account_name ) = 'Direct Wages' OR (chart_of_accounts_dim.account_name ) = 'Sales Wages' OR (chart_of_accounts_dim.account_name ) = 'Trainee Wages' OR (chart_of_accounts_dim.account_name ) = 'Employers National Insurance' OR (chart_of_accounts_dim.account_name ) = 'Pensions Costs')
GROUP BY
    1,
    2,
    3,
    4)
where Employee_Name is not null

{% else %} {{config(enabled=false)}} {% endif %}
