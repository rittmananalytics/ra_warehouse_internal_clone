{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_contract_sources") %}
{% if 'docusign_contracts' in var("projects_warehouse_contract_sources") %}

{% if var("stg_docusign_contracts_etl") == 'fivetran' %}

WITH
  source AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_fivetran_synced) OVER (PARTITION BY envelope_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_fivetran_synced
    FROM
      {{ var('stg_docusign_contracts_contracts_table') }} )
  WHERE
    _fivetran_synced = max_fivetran_synced),
  renamed AS (
  SELECT
    concat('{{ var('stg_docusign_contracts_id-prefix') }}',envelope_id) AS contract_id,
    replace(REPLACE(subject,'Please DocuSign this document: ',''),'Please DocuSign: ','') AS contract_name,
    SPLIT(signer_list,'; ') AS contract_signatories,
    sent_on AS sent_ts,
    status AS contract_status,
    completed_on IS NOT NULL AS is_completed,
    completed_on AS contract_executed_ts,
    voided_on AS contract_voided_on_ts,
    voided_on IS NOT NULL AS is_voided,
    voided_reason,
    _of_completed_signatures AS total_completed_signatures,
    remaining_signatures AS total_remaining_signatures,
    last_activity AS last_modified_date
  FROM
    source),
  split AS (
  SELECT
    * EXCEPT (contract_signatories),
    'Mark Rittman' as authorised_contract_signatory,
    concat('{{ var('stg_docusign_contracts_id-prefix') }}',CASE
      WHEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(1)] IS NULL THEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(0)]
    ELSE
    SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(1)]
  END)
    AS contact_id,
  CASE
      WHEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(1)] IS not null THEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(0)] end as contact_title,
    CASE
      WHEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(1)] IS NULL THEN SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(0)]
    ELSE
    SPLIT(SPLIT(contract_signatories,' [')[safe_OFFSET(0)],' - ')[safe_OFFSET(1)]
  END
    AS contact_name,
    REPLACE(SPLIT(contract_signatories,' [')[safe_OFFSET(1)],']','') AS contact_email
  FROM
    renamed,
    UNNEST (contract_signatories) AS contract_signatories)
SELECT
  *
FROM
  split
WHERE
  contact_name not like '%Rittman%'

  {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
