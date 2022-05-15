{% if var("marketing_warehouse_ad_sources")  %}

{{
    config(
        unique_key='campaign_pk',
        alias='ads_dim'
    )
}}


WITH ads AS
  (
  SELECT * from {{ ref('int_ads') }}
)
,ad_groups as
    (
    SELECT * from {{ ref('wh_ad_groups_dim') }}
  ),
campaigns as (
  SELECT * from {{ ref('wh_ad_campaigns_dim') }}
)
select {{ dbt_utils.surrogate_key(['ad_id']) }} as ad_pk,
       a.* except (adset_id,campaign_id),
       g.adset_pk,
       c.ad_campaign_pk
from ads a
left join ad_groups g
on   a.adset_id = g.adset_id
left join campaigns c
on   a.campaign_id = c.campaign_id







{% else %} {{config(enabled=false)}} {% endif %}
