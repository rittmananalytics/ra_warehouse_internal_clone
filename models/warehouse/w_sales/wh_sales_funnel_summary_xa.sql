{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{{config(alias='sales_funnel_summary_xa',
         materialized="view")}}
  WITH
  funnel_steps AS (
  SELECT
    company_pk,
    funnel_stage,
    event_type,
    MIN(event_ts) OVER (PARTITION BY company_pk, funnel_stage) AS funnel_stage_enter_ts
  FROM
    {{ ref('wh_sales_funnel_xa') }}
  WHERE
    company_pk IS NOT NULL),
  funnel_stages AS (
  SELECT
    company_pk,
    funnel_stage,
    event_type,
    funnel_stage_enter_ts
  FROM
    funnel_steps
  GROUP BY
    1,
    2,
    3,
    4),
  funnel_steps_with_elapsed_ts AS (
  SELECT
    company_pk,
    funnel_stage,
    event_type,
    funnel_stage_enter_ts,
    CASE
      WHEN funnel_stage > LAG(funnel_stage,1) OVER (PARTITION BY company_pk ORDER BY funnel_stage_enter_ts) THEN TIMESTAMP_DIFF( funnel_stage_enter_ts,LAG(funnel_stage_enter_ts,1) OVER (PARTITION BY company_pk ORDER BY funnel_stage_enter_ts),HOUR)
  END
    AS hours_from_last_stage
  FROM
    funnel_stages )
SELECT
  *
FROM
  funnel_steps_with_elapsed_ts
ORDER BY
  company_pk,
  funnel_stage_enter_ts
  {% else %} {{config(enabled=false)}} {% endif %}
