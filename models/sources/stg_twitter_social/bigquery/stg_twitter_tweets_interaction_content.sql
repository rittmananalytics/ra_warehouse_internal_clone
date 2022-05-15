{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'twitter_tweets' in var("marketing_warehouse_interaction_content_sources") %}

WITH content AS (
  select
    *
  from
    {{ source('twitter_tweets','tweets' ) }} p
),
renamed as (
SELECT concat('{{ var('stg_twitter_social_id-prefix') }}',tweetLink) interaction_content_id,
    cast(parse_timestamp('%a %b %d %T %z %Y',tweetDate) as timestamp) interaction_posted_ts,
    text interaction_content,
    'organic_social' as utm_medium,
    'twitter' as utm_source,
    handle as utm_account,
    cast(null as int64) interaction_reported_like_count,
    cast(null as int64) interaction_reported_comment_count,
    cast(null as int64) post_reported_follower_count
FROM content
group by 1,2,3,4,5,6,7,8,9
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
