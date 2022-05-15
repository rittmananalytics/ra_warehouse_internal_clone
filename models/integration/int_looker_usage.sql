
with looker_usage_merge as
  (
    SELECT *
    FROM   {{ ref('stg_looker_usage_stats') }}
  )
select * from looker_usage_merge
