{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{{config(alias='sales_funnel_xa',
         materialized="view")}}

with stage_1_events as (
  select
    1 as funnel_stage,
    blended_user_id as user_id,
    f.company_pk,
    c.contact_pk,
    concat('stage_1_',web_sessions_pk) as event_id,
    session_start_ts as event_ts,
    'site visit' as event_type,
    first_page_url_path as event_details,
    0 as event_value,
    device as device,
    utm_source as source,
    utm_content as content,
    utm_medium as medium,
    utm_campaign as campaign,
    utm_term as keyword,
    referrer_host as referrer_host,
    search as search_term
  from
    {{ ref ('wh_web_sessions_fact') }}  s
  left outer join
    (select
      contact_pk,
      contact_email
     from
      {{ ref ('wh_contacts_dim') }} ,
      unnest (all_contact_emails) as contact_email) c
    on
      s.blended_user_id = c.contact_email
   left outer join
    {{ ref ('wh_contact_companies_fact') }} f
    on
      c.contact_pk = f.contact_pk
),
  stage_2_events as (
    select
      2 as funnel_stage,
      s.blended_user_id as blended_user_id,
      f.company_pk,
      c.contact_pk,
      concat('stage_2_',web_event_pk) as event_id,
      e.event_ts as event_ts,
      'content_interaction' as event_type,
      e.event_type as event_details,
      0 as event_value,
      s.device as device,
      s.utm_source as source,
      s.utm_content as content,
      s.utm_medium as medium,
      s.utm_campaign as campaign,
      s.utm_term as keyword,
      s.referrer_host as referrer_host,
      s.search as search_term
   from
      {{ ref ('wh_web_events_fact') }} e
   join
      {{ ref ('wh_web_sessions_fact') }} s
   on
      e.session_id = s.session_id
   left outer join
    (select
      contact_pk,
      contact_email
     from
      {{ ref ('wh_contacts_dim') }} ,
      unnest (all_contact_emails) as contact_email) c
    on
      s.blended_user_id = c.contact_email
   left outer join {{ ref ('wh_contact_companies_fact') }} f
    on
      c.contact_pk = f.contact_pk
   where
      e.event_type in ('Pricing View','Email Link Clicked','Collateral Viewed','Contact Us Pressed','Form Submitted','Booked A Meeting','Contact Form Submitted')
),
  stage_3_events as (
    with contact_deals_fact as (
      select contact_pk,
             deal_pk
      from {{ ref('wh_contact_deals_fact')}}
           ),
  deals as (
      select
        deal_pk,
        company_pk,
        deal_created_ts,
        deal_closed_ts,
        deal_source,
        deal_pipeline_stage_id,
        deal_total_contract_amount,
        deal_closed_amount_value,
        pipeline_stage_label
   from {{ ref('wh_deals_fact')}})
  select
      3 as funnel_stage,
      cast(null as string) as blended_user_id,
      d.company_pk,
      f.contact_pk,
      concat('stage_3_',d.deal_pk) as event_id,
      deal_created_ts as event_ts,
      'sales_enquiry' as event_type,
      pipeline_stage_label as event_details,
      deal_total_contract_amount as event_value,
      cast(null as string) as device,
      deal_source as source,
      cast(null as string) as content,
      cast(null as string) as medium,
      cast(null as string) as campaign,
      cast(null as string) as keyword,
      cast(null as string) as referrer_host,
      cast(null as string) search_term
  from deals d
  left outer join contact_deals_fact f
  on
    d.deal_pk = f.deal_pk
),

  stage_4_events as (
    select * except (project_seq)
      from (
      select
        4 as funnel_stage,
        cast(null as string) as blended_user_id,
        company_pk as company_id,
        cast(null as string) as email,
        concat('stage_4_',timesheet_project_pk) as event_id,
        timestamp(p.project_delivery_start_ts) as event_ts,
        'trial_project' as event_type,
        p.project_code as event_details,
        project_fee_amount as event_value,
        cast(null as string) as device,
        cast(null as string) as source,
        cast(null as string) as content,
        cast(null as string) as medium,
        cast(null as string) as campaign,
        cast(null as string) as keyword,
        cast(null as string) as referrer_host,
        cast(null as string) as search_term,
        row_number() over (partition by p.company_pk order by timestamp(p.project_delivery_start_ts) )  as project_seq
     from
        {{ ref ('wh_timesheet_projects_dim') }} p
   )
   where project_seq = 1
   and event_ts is not null
),
stage_5_events as (
    select * except (project_seq)
      from (
      select
        5 as funnel_stage,
        cast(null as string) as blended_user_id,
        company_pk as company_id,
        cast(null as string) as email,
        concat('stage_5_',timesheet_project_pk) as event_id,
        timestamp(p.project_delivery_start_ts) as event_ts,
        'subscribed' as event_type,
        p.project_code as event_details,
        project_fee_amount as event_value,
        cast(null as string) as device,
        cast(null as string) as source,
        cast(null as string) as content,
        cast(null as string) as medium,
        cast(null as string) as campaign,
        cast(null as string) as keyword,
        cast(null as string) as referrer_host,
        cast(null as string) as search_term,
        row_number() over (partition by p.company_pk order by timestamp(p.project_delivery_start_ts) ) as project_seq
     from
        {{ ref ('wh_timesheet_projects_dim') }} p
   )
   where project_seq = 2
   and event_ts is not null
),
stage_6_events as (
    select * except (project_seq)
      from (
      select
        6 as funnel_stage,
        cast(null as string) as blended_user_id,
        company_pk as company_id,
        cast(null as string) as email,
        concat('stage_7_',timesheet_project_pk) as event_id,
        timestamp(p.project_delivery_start_ts) as event_ts,
        'renewed' as event_type,
        p.project_code as event_details,
        project_fee_amount as event_value,
        cast(null as string) as device,
        cast(null as string) as source,
        cast(null as string) as content,
        cast(null as string) as medium,
        cast(null as string) as campaign,
        cast(null as string) as keyword,
        cast(null as string) as referrer_host,
        cast(null as string) as search_term,
        row_number() over (partition by p.company_pk order by timestamp(p.project_delivery_start_ts) ) as project_seq
     from
        {{ ref ('wh_timesheet_projects_dim') }} p
   )
   where project_seq > 2
   and event_ts is not null
),
all_stages as (
  select * from stage_1_events
    union all
  select * from stage_2_events
    union all
  select * from stage_3_events
    union all
  select * from stage_4_events
    union all
  select * from stage_5_events
    union all
  select * from stage_6_events
)
select
  *
from
  all_stages
order by
  company_pk, funnel_stage, event_ts
{% else %} {{config(enabled=false)}} {% endif %}
