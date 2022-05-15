{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_interaction_content_sources") %}
{% if 'linkedin_posts' in var("marketing_warehouse_interaction_content_sources") %}

WITH posts AS (
  SELECT * FROM (
    SELECT
      Row_Updated_At,
      postUrl,
      profileUrl,
      fullName,
      title,
      max(postDate) over (partition by postUrl) postDate,
      textContent,
      max(likeCount) over (partition by postUrl) likeCount,
      max(commentCount) over (partition by postUrl) commentCount,
      query,
      category,
      timestamp,
      name,
      companyUrl,
      companyName,
      max(followerCount) over (partition by followerCount) followerCount
    FROM
  {{ source('linkedin_posts','posts' ) }}
)
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  )
,
renamed as (
  SELECT
    concat('{{ var('stg_linkedin_social_id-prefix') }}',SPLIT(postUrl,'activity:')[safe_OFFSET(1)]) AS interaction_content_id,
    case
      when postDate like '%mo%' then timestamp(date_sub(current_date,interval safe_cast(split(postDate,'mo')[safe_offset(0)] as int64) month))
      when postDate like '%yr%' then timestamp(date_sub(current_date,interval safe_cast(split(postDate,'yr')[safe_offset(0)] as int64) year))
      when postDate like '%d%' then timestamp(date_sub(current_date,interval safe_cast(split(postDate,'d')[safe_offset(0)] as int64) day))
    end as interaction_posted_ts  ,
    textContent as interaction_content,
    'organic_social' as utm_medium,
    'linkedin' as utm_source,
    'markrittman' as utm_account,
    coalesce(likeCount,0) as interaction_reported_like_count,
    coalesce(commentCount,0) as interaction_reported_comment_count,
    coalesce(safe_cast(followerCount as int64),0) as post_reported_follower_count
FROM
  posts
WHERE
  name = 'Mark Rittman'
  and postUrl is not null
)
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
