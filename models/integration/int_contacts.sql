{% if var('crm_warehouse_contact_sources') %}

{{config(materialized="table")}}

with t_contacts_merge_list as (
  SELECT
    *
  FROM
    {{ ref('int_contacts_pre_merged') }}
),
{% if target.type == 'bigquery' %}

    contact_emails as (
             SELECT contact_name, array_agg(distinct lower(contact_email) ignore nulls) as all_contact_emails
             FROM t_contacts_merge_list
             group by 1),
    contact_jobs as (
                      SELECT contact_name, array_agg(distinct lower(job_title) ignore nulls) as all_job_titles
                      FROM t_contacts_merge_list
                      group by 1),


    contact_ids as (
             SELECT contact_name, array_agg(distinct contact_id ignore nulls) as all_contact_ids
             FROM t_contacts_merge_list
             group by 1),
    contact_company_ids as (
                   SELECT contact_name, array_agg(contact_company_id ignore nulls) as all_contact_company_ids
                   FROM t_contacts_merge_list
                   group by 1),
    contact_company_addresses as (
             select contact_name, ARRAY_AGG(STRUCT( contact_address, contact_city, contact_state, contact_country, contact_postcode_zip) IGNORE NULLS) as all_contact_addresses
             FROM t_contacts_merge_list
             where contact_address is not null
             group by 1),

{% elif target.type == 'snowflake' %}

    contact_emails as (
             SELECT contact_name, array_agg(distinct lower(contact_email)) as all_contact_emails
             FROM t_contacts_merge_list
             group by 1),
    contact_jobs as (
                      SELECT contact_name, array_agg(distinct lower(job_title)) as all_job_titles
                      FROM t_contacts_merge_list
                      group by 1),

    contact_ids as (
             SELECT contact_name, array_agg(distinct contact_id) as all_contact_ids
             FROM t_contacts_merge_list
             group by 1),
    contact_company_ids as (
                   SELECT contact_name, array_agg(contact_company_id) as all_contact_company_ids
                   FROM t_contacts_merge_list
                   group by 1),
    contact_company_addresses as (
             select contact_name,
                       array_agg(
                            parse_json (
                              concat('{"contact_address":"',contact_address,
                                     '", "contact_city":"',contact_city,
                                     '", "contact_state":"',contact_state,
                                     '", "contact_country":"',contact_country,
                                     '", "contact_postcode_zip":"',contact_postcode_zip,'"} ')
                            )
                       ) as all_contact_addresses
             FROM t_contacts_merge_list

             group by 1),

{% else %}
      {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

{% endif %}

contacts as (
   select all_contact_ids,
          c.contact_name,
          c.contact_bio,
          contact_phone,
          contact_is_contractor,
          contact_is_staff,
          contact_weekly_capacity,
          contact_default_hourly_rate,
          contact_cost_rate,
          contact_is_active,
          contact_created_date,
          contact_last_modified_date,
          contact_friends_count,
          contact_posts_count,
          contact_is_following,
          contact_is_followed_by_us,
          contact_job_description,
          contact_school,
          contact_description,
          contact_subscribers,
          contact_connection_degree,
          contact_connections_count,
          contact_mutual_connections,
          contact_mail_from_drop_contact,
          contact_school_degree,
          contact_school_description,
          contact_qualifications,
          contact_skills,
          e.all_contact_emails,
          j.all_job_titles,
          a.all_contact_addresses,
          cc.all_contact_company_ids
         from (
            select contact_name,
                max(contact_bio) as contact_bio,
                max(contact_phone) as contact_phone,
                min(contact_created_date) as contact_created_date,
                max(contact_last_modified_date) as contact_last_modified_date,
                max(contact_is_contractor)         as contact_is_contractor,
                max(contact_is_staff) as contact_is_staff,
                max(contact_weekly_capacity)          as contact_weekly_capacity,
                max(contact_default_hourly_rate)          as contact_default_hourly_rate,
                max(contact_cost_rate)           as contact_cost_rate,
                max(contact_is_active)                          as contact_is_active,
                max(contact_is_following) as contact_is_following,
                max(contact_is_followed_by_us) as contact_is_followed_by_us,
                max(contact_friends_count) as contact_friends_count,
                max(contact_posts_count) as contact_posts_count,
                max(contact_job_description) as contact_job_description,
                max(contact_school) as contact_school,
                max(contact_description) as contact_description,
                max(contact_subscribers) as contact_subscribers,
                max(contact_connection_degree) as contact_connection_degree,
                max(contact_connections_count) as contact_connections_count,
                max(contact_mutual_connections) as contact_mutual_connections,
                max(contact_mail_from_drop_contact) as contact_mail_from_drop_contact,
                max(contact_school_degree) as contact_school_degree,
                max(contact_school_description) as contact_school_description,
                max(contact_qualifications) as contact_qualifications,
                max(contact_skills) as contact_skills
            FROM t_contacts_merge_list
         group by 1) c
  join contact_emails e on c.contact_name = e.contact_name
  left join contact_jobs j on c.contact_name = j.contact_name
  join contact_ids i on c.contact_name = i.contact_name
  left join contact_company_addresses a on c.contact_name = a.contact_name
  left join contact_company_ids cc on c.contact_name = cc.contact_name)
select * from contacts

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
