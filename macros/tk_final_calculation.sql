{% macro tk_final_calculation() %}
ad_stat_id AS (
SELECT 
  SUM(ad.clicks) AS clicks,
  SUM(ad.media_cost) as media_cost,
  SUM(ad.impressions) AS impressions,
  SUM(ad_v.video_25_completion) as video_25_completion,
  SUM(ad_v.video_50_completion) as video_50_completion,
  SUM(ad_v.video_75_completion) as video_75_completion,
  SUM(ad_v.video_100_completion) as video_completion,
  SUM(ad_v.video_play) as video_play,
  SUM(ad_v.video_views) as video_views,
  ad.ad_id as ad_id,
  ad.date
FROM deduplicate_ad as ad LEFT JOIN deduplicate_ad_video as ad_v on ad.ad_id = ad_v.ad_id 
AND ad.date = ad_v.date
GROUP BY date, ad_id),
final as (
SELECT ad_stat_id.* EXCEPT(ad_id),
ad_campaign.* FROM ad_stat_id LEFT JOIN ad_campaign ON SAFE_CAST(ad_stat_id.ad_id AS INT64) = SAFE_CAST(ad_campaign.ad_id AS INT64)
)
SELECT  * EXCEPT(date,ad_name), DATE(PARSE_DATETIME('%F %H:%M:%S',date)) AS date,
ad_name as creative_name,
    'Tiktok' AS publisher,
    REGEXP_EXTRACT(adgroup_name, r'PLATFORM_([^_]+)') AS audience_name,
    CASE 
        WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=4 AND SPLIT (campaign_name,'_')[OFFSET(3)] LIKE '%SOCIAL%'
        AND (
            lower(ad_name) LIKE '%vid%'
            OR lower(campaign_name) LIKE '%vid%'
            OR lower(adgroup_name) LIKE '%vid%'
        ) THEN 'Social Video'
        WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=4 AND SPLIT (campaign_name,'_')[OFFSET(3)] LIKE '%SOCIAL%'
        AND (
            lower(ad_name) NOT LIKE '%vid%'
            AND lower(campaign_name) NOT LIKE '%vid%'
            AND lower(adgroup_name) NOT LIKE '%vid%'
        )
        THEN 'Social Display'
        ELSE 'Other'
    END AS media_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(ad_name,'_'))>=8 THEN SPLIT(ad_name, '_')[OFFSET(5)] ELSE 'Other' END AS ad_format_detail,
    CASE WHEN ARRAY_LENGTH(SPLIT(ad_name,'_'))>=8 THEN SPLIT(ad_name, '_')[OFFSET(6)] ELSE 'Other' END AS ad_format,
    SPLIT(ad_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(ad_name, '_'))-1)] AS creative_descr,
    SPLIT(campaign_name,'_')[OFFSET(1)] AS campaign_descr
FROM 
    final

{% endmacro %}
    