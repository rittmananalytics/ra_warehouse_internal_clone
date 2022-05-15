{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") and var("crm_warehouse_nps_sources") %}
{{config(alias='contact_nps_survey_fact',
         materialized="view")}}
with contacts_dim as (
             select *
             from {{ ref('wh_contacts_dim') }}
           ),
     nps_survey_responses as (
       select *
       from {{ ref('int_nps_surveys') }}
     )
         SELECT
            GENERATE_UUID() as nps_survey_pk,
            p.contact_pk,
            r.* except (contact_id,nps_survey_id)
         FROM
            nps_survey_responses r
         JOIN contacts_dim p
            ON r.contact_id IN UNNEST(p.all_contact_ids)
{% else %} {{config(enabled=false)}} {% endif %}
