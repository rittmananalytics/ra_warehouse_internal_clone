{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'xero_accounting' in var("crm_warehouse_contact_sources") %}

{% if var("stg_xero_accounting_etl") == 'stitch' %}

WITH xero_contacts as (
  {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_contacts_table'),unique_column='contactid') }}
),

contacts as (

  WITH phones as (SELECT contacts.contactid, phones.phonetype, phones.phonenumber, phones.phoneareacode, phones.phonecountrycode
    FROM xero_contacts contacts,
 unnest(phones) as phones
 ),
      addresses as (SELECT contacts.contactid, addresses.addresstype, addresses.addressline1, addresses.addressline2, addresses.addressline3, addresses.addressline4, addresses.city, addresses.region, addresses.country, addresses.postalcode
         FROM xero_contacts contacts,
 unnest(addresses) as addresses
 )
 select
        concat('{{ var('stg_xero_accounting_id-prefix') }}',contacts.contactid) as contact_id,
        contacts.firstname as contact_first_name,
        contacts.lastname as contact_last_name,
        cast(null as string) as contact_job_title,
        coalesce(concat(contacts.firstname,' ',contacts.lastname),contacts.emailaddress) as contact_name,
        contacts.emailaddress as contact_email,
        replace(concat(replace(defaultphone.phonecountrycode,'+','00'),defaultphone.phoneareacode,defaultphone.phonenumber),' ','') as contact_phone,
        string_agg(distinct addresses.addressline1) as contact_address,
        string_agg(distinct addresses.city) as contact_city,
        string_agg(distinct addresses.region) as contact_state,
        string_agg(distinct addresses.country) as contact_country,
        string_agg(distinct addresses.postalcode) as contact_postcode_zip,
        cast(null as string) as contact_company,
        cast(null as string) as contact_website,
        cast(null as string) as contact_company_id,
        cast(null as string) as contact_owner_id,
        contacts.contactstatus as contact_lifecycle_stage,
        cast(null as boolean)         as contact_is_contractor,
        cast(null as boolean) as contact_is_staff,
        cast(null as int64)           as contact_weekly_capacity,
        cast(null as int64)           as contact_default_hourly_rate,
        cast(null as int64)           as contact_cost_rate,
        CAST(NULL AS STRING) AS contact_bio,
        cast(null as int64) AS contact_friends_count,
        cast(null as int64) AS contact_posts_count,
        cast(null as boolean)  contact_is_following,
        cast(null as boolean)  contact_is_followed_by_us,
        false                          as contact_is_active,
        cast(null as timestamp) as contact_created_date,
        contacts.updateddateutc as contact_last_modified_date
 from xero_contacts contacts
 left outer join addresses as addresses
 on contacts.contactid = addresses.contactid
 and addresses.addresstype = 'STREET'
 left outer join phones as mobilephone
 on contacts.contactid = mobilephone.contactid
 and mobilephone.phonetype = 'MOBILE'
 left outer join phones as defaultphone
 on contacts.contactid = defaultphone.contactid
 and mobilephone.phonetype = 'DEFAULT'
 where concat(contacts.firstname,' ',contacts.lastname) is not null
 group by 1,2,3,4,5,6,7,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
)

{% elif var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_xero_accounting_fivetran_contacts_table') }}

),
      addresses as (SELECT contact_id, address_type, address_line_1, address_line_2, address_line_3, address_line_4, city, region, country, postal_code
         FROM {{ var('stg_xero_accounting_fivetran_contact_address_table') }}

),
renamed as (
 select
     concat('{{ var('stg_xero_accounting_id-prefix') }}',contacts.contact_id) as contact_id,
     contacts.first_name as contact_first_name,
     contacts.last_name as contact_last_name,
     cast(null as string) as contact_job_title,
     coalesce(concat(contacts.first_name,' ',contacts.last_name),contacts.email_address) as contact_name,
     contacts.email_address as contact_email,
     cast(null as string) as company_phone,
     string_agg(distinct addresses.address_line_1) as contact_address,
     string_agg(distinct addresses.city) as contact_city,
     string_agg(distinct addresses.region) as contact_state,
     string_agg(distinct addresses.country) as contact_country,
     string_agg(distinct addresses.postal_code) as contact_postcode_zip,
     cast(null as string) as contact_company,
     cast(null as string) as contact_website,
     cast(null as string) as contact_company_id,
     cast(null as string) as contact_owner_id,
     contacts.contact_status as contact_lifecycle_stage,
     cast(null as boolean)         as contact_is_contractor,
     cast(null as boolean) as contact_is_staff,
     cast(null as int64)           as contact_weekly_capacity,
     cast(null as int64)           as contact_default_hourly_rate,
     cast(null as int64)           as contact_cost_rate,
     CAST(NULL AS STRING) AS contact_bio,
     cast(null as int64) AS contact_friends_count,
     cast(null as int64) AS contact_posts_count,
     cast(null as boolean)  contact_is_following,
     cast(null as boolean)  contact_is_followed_by_us,
     false                          as contact_is_active,
     cast(null as timestamp) as contact_created_date,
     timestamp(contacts.updated_date_utc) as contact_last_modified_date
 from source as contacts
 left outer join addresses as addresses
 on contacts.contact_id = addresses.contact_id and addresses.address_type = 'STREET'
 where concat(contacts.first_name,' ',contacts.last_name) is not null
 group by 1,2,3,4,5,6,7,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30)


{% endif %}

select * from renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
