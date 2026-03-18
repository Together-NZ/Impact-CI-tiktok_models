{% macro tk_video_metrics_by_day(source_name, table_name) %}
ad_video AS (
  SELECT 
    SAFE_CAST(JSON_VALUE(data, '$.ad_id') AS INT64) AS ad_id,
    SAFE_CAST(JSON_VALUE(data, '$.video_views_p25') AS INT64) AS video_25_completion,   
    SAFE_CAST(JSON_VALUE(data, '$.video_views_p50') AS INT64) AS video_50_completion,
    SAFE_CAST(JSON_VALUE(data, '$.video_views_p75') AS INT64) AS video_75_completion,
    SAFE_CAST(JSON_VALUE(data, '$.video_views_p100') AS INT64) AS video_100_completion,
    SAFE_CAST(JSON_VALUE(data, '$.video_play_actions') AS INT64) AS video_play,
    SAFE_CAST(JSON_VALUE(data, "$.video_watched_2s") AS INT64) AS video_views,
    JSON_VALUE(data, '$.stat_time_day') AS date,
    ROW_NUMBER() OVER (PARTITION BY SAFE_CAST(JSON_VALUE(data, '$.ad_id') AS INT64),JSON_VALUE(data, '$.stat_time_day')) as row_num
  FROM {{ source(source_name, table_name) }}
),
deduplicate_ad_video as (
  select * from ad_video where row_num = 1
)
{% endmacro %}