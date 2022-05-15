{%- macro generate_metrics_schema(table_schema,model_prefix,pk_suffix) -%}

{% if target.type == 'bigquery' %}

{% set schema_yml %}

/*

   Auto-generates the Lightdash metrics layer schema.yml file
   Assumptions and pre-requisites are:

   1. Cloud data warehouse is Google Bigquery
   - because of dependency on INFORMATION_SCHEMA, though it may still work with Snowflake etc
   - suggested "bq query" method of executing the generated SQL statement, though snowsql will probably work too
   - STRING and other BQ-specific datatypes, though these could be changed to VARCHAR etc if running on Snowflake
   2. All metric columns have a datatype of FLOAT64 or NUMERIC

   Run this macro using the following CLI command:

    dbt run-operation generate_metrics_schema --args '{table_schema: analytics, model_prefix: wh_, pk_suffix: _pk}' | \
     tail -n +3 | \
     bq query -use_legacy_sql=false -format sparse | \
     tail -n +3 > schema.yml

*/

WITH
  information_schema as (
    select
      *
    from
      {{ table_schema }}.INFORMATION_SCHEMA.COLUMNS
  )
  ,
    table_name as (
      select
        table_name,
        CONCAT("  - name: ","{{ model_prefix }}",table_name,"\n","    meta:","\n",'      label: "',initcap(replace(table_name,'_',' ')),'"',"\n","    columns:") AS schema_line,
        1 as line_order
      from
        information_schema
      group by
        1,2,3
  )
  ,
    string_dimensions AS (
      select
        table_name,
        concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(column_name,'_',' ')),'"',"\n","            type: string","\n","            hidden: no") as schema_line,
        2 as line_order
      from
        information_schema
      where
        data_type = 'STRING'
      and
        (column_name not like '{{ pk_column_suffix }}')
    )
    ,
      key_dimensions AS (
    select
      table_name,
      concat("      - name: ",column_name,"\n        tests:\n          - unique\n          - not_null","\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(column_name,'_',' ')),'"',"\n","            type: string","\n","            hidden: yes","\n","          metrics:","\n","            count_",column_name,":","\n",'              label: "',"Count ",initcap(replace(column_name,'_',' ')),'"',"\n","              type: count_distinct") AS schema_line,
      3 AS line_order
    from
      information_schema
    where
      column_name like '{{ pk_column_suffix }}'
    )
  ,
    timestamp_dimensions AS (
    select
      table_name,
      concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(replace(column_name,'_ts',''),'_',' ')),'"',"\n","            type: timestamp","\n","            time_intervals: default") AS schema_line,
      4 AS line_order
    from
      information_schema
    where
      data_type in ('TIMESTAMP')
  )
  ,
    date_dimensions AS (
    select
      table_name,
      concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(replace(column_name,'_date',''),'_',' ')),'"',"\n","            type: date","\n","            time_intervals: default") AS schema_line,
      5 AS line_order
    from
      information_schema
    where
      data_type in ('DATE')
    ),
    boolean_dimensions AS (
    select
      table_name,
      concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(column_name,'_',' ')),'"',"\n","            type: boolean","\n","            hidden: no") AS schema_line,
      6 AS line_order
    from
      information_schema
    where
      data_type = 'BOOL'
  )
  ,
      metrics AS (
  SELECT
    table_name,
    concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(column_name,'_',' ')),'"',"\n","            type: number","\n","            hidden: yes","\n","          metrics:","\n","            total_",column_name,":","\n","              type: sum","\n","              label: ",'"',"Total ",initcap(replace(column_name,'_',' ')),'"',"\n","            avg_",column_name,":","\n","              type: average","\n","              label: ",'"',"Average ",initcap(replace(column_name,'_',' ')),'"') AS schema_line,
    8 AS line_order
  FROM
    information_schema
  WHERE
    data_type IN ('FLOAT64',
      'NUMERIC',
      'BIGNUMERIC')
  )
  ,
  numeric_dimensions AS (
    select
      table_name,
      concat("      - name: ",column_name,"\n","        meta:","\n","          dimension:","\n",'            label: "',initcap(replace(column_name,'_',' ')),'"',"\n","            type: numeric","\n","            hidden: no") AS schema_line,
      7 AS line_order
    from
      information_schema
    where
      data_type IN ('INT64')),
  lines as (
    select
      *
    from
      table_name
    union all
      select
        *
      from
        key_dimensions
    union all
      select
        *
      from
        string_dimensions
    union all
      select
        *
      from
        timestamp_dimensions
    union all
      select
        *
      from
        date_dimensions
    union all
      select
        *
      from
        numeric_dimensions
    union all
      select
        *
      from
        boolean_dimensions
    union all
      select
        *
      from
        metrics),
  models as (
    select
      table_name,
      replace(string_agg(concat(schema_line,"\n")
        order by line_order),',','') as schema_yml
    from
      lines
    group by 1
  ),
  schema as (
    select
      concat("version: 2\nmodels:\n\n",replace(string_agg(concat(schema_yml,"\n")
      order by table_name),',','')) as schema
  from
    models)
select
  *
from
  schema

{% endset %}

{% if execute %}

  {{ log(schema_yml, info=True) }}
  {% do return(schema_yml) %}

{% endif %}

{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported for this macro") }}
{% endif %}

{%- endmacro -%}
