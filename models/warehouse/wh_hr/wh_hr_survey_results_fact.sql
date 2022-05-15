{% if var("hr_warehouse_survey_sources")  %}

{{
    config(
        alias='hr_survey_results_fact'
    )
}}


WITH surveys AS
  (
  SELECT * from {{ ref('int_hr_surveys') }}
)
select {{ dbt_utils.surrogate_key(['survey_ts']) }} as survey_pk,
       s.*
from surveys s


{% else %} {{config(enabled=false)}} {% endif %}
