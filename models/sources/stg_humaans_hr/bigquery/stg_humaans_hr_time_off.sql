{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'humaans_hr' in var("hr_warehouse_timeoff_sources") %}

WITH source AS (
  select
    *
  from
    {{ source('humaans_hr','time_off' ) }}
),
renamed as (
select
  data_id as time_off_id,
  concat('{{ var('stg_humaans_hr_id-prefix') }}',data_Personid) as contact_id,
  data_type as time_off_type_id,
  data_name as time_off_type_name,
  data_isTimeOff is_time_off,
  timestamp(data_startDate) as time_off_start_ts,
  timestamp(data_endDate) as time_off_end_ts,
  data_note as time_off_note,
  data_days as days_time_off,
  data_requestStatus = 'approved' as is_time_off_approved
from source
)
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
