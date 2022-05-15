{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'linkedin_commenters' in var("crm_warehouse_contact_sources") %}

with contacts as (
  select
    *
  from
  (
    select
      case when firstname is not null then concat(firstname,' ',lastname) else null end as fullname,
      firstname,
      lastname,
      occupation as job,
      degree
    from {{ source('linkedin_commenters','contacts' ) }}
  )
  group by 1,2,3,4,5
),
  profiles as (
    select
      *
    from
      {{ source('linkedin_followers','profiles' ) }}
  ),
renamed as (
select
  concat('{{ var('stg_linkedin_social_id-prefix') }}',concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', ''))) as contact_id,
  REGEXP_REPLACE(c.firstName, r'\W', '') contact_first_name,
  REGEXP_REPLACE(c.lastName, r'\W', '') contact_last_name,
  concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) contact_name,
  cast(coalesce(p.jobtitle,c.job) as string) as  contact_job_title,
  cast(p.email as string) as  contact_email,
  coalesce(p.phonenumber,cast(null as string)) as  contact_phone,
  coalesce(p.location,cast(null as string)) as  contact_address,
  cast(null as string) as contact_city,
  cast(null as string) as contact_state,
  cast(null as string) as contact_country,
  cast(null as string) as contact_postcode_zip,
  coalesce(p.company,cast(null as string)) as  contact_company,
  coalesce(p.companywebsite,cast(null as string)) as contact_website,
  cast(null as string) as contact_company_id,
  cast(null as string) as contact_owner_id,
  cast(null as string) as contact_lifecycle_stage,
  cast(null as boolean) as contact_is_contractor,
  cast(null as boolean) as contact_is_staff,
  cast(null as int64) as contact_weekly_capacity,
  cast(null as int64) as contact_default_hourly_rate,
  cast(null as int64) as contact_cost_rate,
  cast(null as string) as contact_bio,
  cast(null as int64) as contact_friends_count,
  cast(null as int64) as contact_posts_count,
  false as  contact_is_following,
  false as  contact_is_followed_by_us,
  cast(null as boolean) as contact_is_active,
  cast(p.jobdescription as string) as contact_job_description,
  cast(p.school as string) as  contact_school,
  cast(p.description as string) as contact_description,
  cast(p.subscribers as int64) as contact_subscribers,
  cast(concat(p.connectiondegree,c.degree) as string) as contact_connection_degree,
  cast(p.connectionscount as int64) as contact_connections_count,
  cast(p.mutualconnectionstext as string) contact_mutual_connections,
  cast(p.mailfromdropcontact as string) as contact_mail_from_drop_contact,
  cast(p.schooldegree as string) as contact_school_degree,
  cast(p.schooldescription as string) as contact_school_description,
  cast(p.qualificationfromdropcontact as string) as  contact_qualifications,
  cast(p.allskills as string) as contact_skills,
  cast(null as timestamp) as contact_created_date,
  cast(null as timestamp) as contact_last_modified_date
from
  contacts c
left join
    profiles p
  on
    concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) = p.fullname
where concat(REGEXP_REPLACE(c.firstName, r'\W', ''),' ',REGEXP_REPLACE(c.lastName, r'\W', '')) is not null
{{ dbt_utils.group_by(42) }}
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
