{% macro tk_ads(source_name, ad_table_name, campaign_table_name) %}
ad_campaign_duplicate AS (SELECT 
trim(JSON_VALUE(data,'$.ad_id')) as ad_id,
trim(JSON_VALUE(data,'$.ad_name')) as ad_name,
trim(JSON_VALUE(data,'$.campaign_id')) as campaign_id,
trim(JSON_VALUE(data,'$.adgroup_id')) as adgroup_id,
ROW_NUMBER() OVER (PARTITION BY trim(JSON_VALUE(data,'$.ad_id')) ORDER BY trim(JSON_VALUE(data,'$.modify_time')) DESC) as row_num
FROM {{ source(source_name, ad_table_name) }} ),
ad_data AS (
  SELECT * FROM ad_campaign_duplicate WHERE row_num = 1
),
campaign_data AS (
  SELECT TRIM(JSON_VALUE(data,'$.campaign_id')) AS campaign_id,
  TRIM(JSON_VALUE(data,'$.campaign_name')) AS campaign_name,
  _sdc_extracted_at as _sdc_extracted_at,
  FROM {{ source(source_name, campaign_table_name) }}
  ROW_NUMBER() OVER (PARTITION BY TRIM(JSON_VALUE(data,'$.campaign_id')) ORDER BY _sdc_extracted_at DESC) as row_num
),
deduplicate_campaign_data AS (
  SELECT * FROM campaign_data WHERE row_num = 1
),
ad_campaign_no_adgroup as (
  SELECT ad_campaign.* except(campaign_id), deduplicate_campaign_data.* FROM ad_campaign LEFT JOIN deduplicate_campaign_data ON ad_campaign.campaign_id = deduplicate_campaign_data.campaign_id
),
adgroup_data AS (
  SELECT TRIM(JSON_VALUE(data,'$.adgroup_id')) AS adgroup_id,
  TRIM(JSON_VALUE(data,'$.adgroup_name')) AS adgroup_name,
  _sdc_extracted_at as _sdc_extracted_at,
  FROM {{ source(source_name, adgroup_table_name) }}
  ROW_NUMBER() OVER (PARTITION BY TRIM(JSON_VALUE(data,'$.adgroup_id')) ORDER BY _sdc_extracted_at DESC) as row_num
),
deduplicate_adgroup_data AS (
  SELECT * FROM adgroup_data WHERE row_num = 1
),
ad_campaign AS (
  SELECT ad_campaign_no_adgroup.* except(adgroup_id,campaign_id), deduplicate_adgroup_data.* FROM ad_campaign_no_adgroup LEFT JOIN deduplicate_adgroup_data ON ad_campaign_no_adgroup.adgroup_id = deduplicate_adgroup_data.adgroup_id
)

{% endmacro %}