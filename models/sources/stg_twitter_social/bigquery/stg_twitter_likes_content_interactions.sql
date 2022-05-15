{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_content_interaction_sources") %}
{% if 'twitter_likes' in var("marketing_warehouse_content_interaction_sources") %}

WITH interactions AS (
  select
    *
  from
    {{ source('twitter_tweets','twitter_likes' ) }}
  )
,
content  as (
  select
    *
  from
    {{ ref('stg_twitter_tweets_interaction_content') }}
  ),
renamed as (
  SELECT
      concat('{{ var('stg_twitter_social_id-prefix') }}',i.screenName) as contact_id,
    concat('{{ var('stg_twitter_social_id-prefix') }}',i.tweetUrl) as interaction_content_id,
    i.tweetUrl as interaction_content_url,
    cast(null as string) as interaction_content_title,
    i.action as interaction_event_name,
    cast(null as string) as interaction_event_details,
    c.interaction_posted_ts as interaction_event_ts
  FROM
    interactions i
  JOIN
    content c
  ON
    concat('{{ var('stg_twitter_social_id-prefix') }}',i.tweetUrl) = c.interaction_content_id
  group by
    1,2,3,4,5,6,7
)
select
  *
from
  renamed c

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
