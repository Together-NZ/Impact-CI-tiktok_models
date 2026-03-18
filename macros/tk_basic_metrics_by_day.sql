{% macro tk_basic_metrics_by_day(source_name, table_name) %}
ad_data AS (
  SELECT 
    SAFE_CAST(JSON_VALUE(data, '$.ad_id') AS INT64) AS ad_id,
    SAFE_CAST(JSON_VALUE(data,'$.spend') AS FLOAT64) AS media_cost,
    SAFE_CAST(JSON_VALUE(data, '$.impressions') AS INT64) AS impressions,
    SAFE_CAST(JSON_VALUE(data, '$.clicks') AS INT64) AS clicks,
    JSON_VALUE(data, '$.stat_time_day') AS date,
    ROW_NUMBER() OVER (PARTITION BY SAFE_CAST(JSON_VALUE(data, '$.ad_id') AS INT64),JSON_VALUE(data, '$.stat_time_day')) as row_num
  FROM {{ source(source_name, table_name) }}
),
deduplicate_ad as (
  select * from ad_data where row_num = 1
)
{% endmacro %}
