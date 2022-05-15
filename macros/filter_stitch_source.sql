{%- macro filter_stitch_source(schema_name, source_name, table_name, unique_column) -%}

SELECT
  * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
FROM
  (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY {{ unique_column }} ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source(source_name,table_name) }}
  )
WHERE
  _sdc_batched_at = max_sdc_batched_at

{%- endmacro -%}
