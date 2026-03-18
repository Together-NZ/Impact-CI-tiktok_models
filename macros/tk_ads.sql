{% macro tk_ads(source_name, table_name) %}
ad_campaign_duplicate AS (SELECT 
trim(JSON_VALUE(data,'$.ad_id')) as ad_id,
trim(JSON_VALUE(data,'$.ad_name')) as ad_name,
trim(JSON_VALUE(data,'$.campaign_name')) as campaign_name,
trim(JSON_VALUE(data,'$.adgroup_name')) as adgroup_name,
ROW_NUMBER() OVER (PARTITION BY trim(JSON_VALUE(data,'$.ad_id')) ORDER BY trim(JSON_VALUE(data,'$.modify_time')) DESC) as row_num
FROM {{ source(source_name, table_name) }} ),
ad_campaign AS (
  SELECT * FROM ad_campaign_duplicate WHERE row_num = 1
)
{% endmacro %}