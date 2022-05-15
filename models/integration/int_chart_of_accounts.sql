{% if var('finance_warehouse_payment_sources') %}


with t_chart_of_accounts_merge_list as
  (
    {% for source in var('finance_warehouse_payment_sources') %}
      {% set relation_source = 'stg_' + source + '_accounts' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT t.*,
       g.report_order as account_report_order,
       g.category as account_report_category,
       g.category_order as account_report_category_order,
       g.sub_category as account_report_sub_category,
       g.sub_category_order as account_report_sub_category_order,
       g.account_group as account_report_group
from t_chart_of_accounts_merge_list t
left join {{ ref('profit_and_loss_groupings') }} g
on t.account_code = cast(g.account_code as string)

{% else %} {{config(enabled=false)}} {% endif %}
