{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{{
    config(
        alias='conversations_fact'
    )
}}


with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
),
    contacts_dim as (
     SELECT *
      FROM
      (SELECT
        c.contact_pk,
        contact_email,
        contact_name,
        c.all_contact_ids,
       FROM {{ ref('wh_contacts_dim') }} c,
       UNNEST( all_contact_emails ) as contact_email
        )

    ),
  conversations as (
SELECT

   p.contact_pk,
   f.contact_pk as from_contact_pk,
   t.contact_pk as to_contact_pk,
   t.contact_name as conversation_to_contact_name,
   m.* except (conversation_author_id,conversation_user_id,conversation_assignee_id)
FROM
   {{ ref('int_conversations') }} m
JOIN contacts_dim p
   ON m.conversation_author_id IN UNNEST(p.all_contact_ids)
JOIN contacts_dim f
   ON m.conversation_from_email = p.contact_email
JOIN contacts_dim t
   ON m.conversation_to_email = t.contact_email
{{ dbt_utils.group_by(n=22) }})
SELECT GENERATE_UUID() as conversation_pk,
       *
FROM   conversations
where  to_contact_pk is not null


{% else %} {{config(enabled=false)}} {% endif %}
