{{
    config
	(
		materialized='table'	
    )
}}
with
facebook as (
select distinct
      date, add_to_cart, clicks, 
      comments+comments_2 as comments,  (comments + shares + likes + inline_link_clicks+mobile_app_install+views) as engagements, impressions,
      mobile_app_install as  installs, likes, inline_link_clicks as link_clicks,  0 as post_click_conversions, 0 as post_view_conversions,
      0 as posts, purchase, complete_registration as registrations, 0 as revenue, (shares+shares_2) as shares, spend, 
      (purchase_2) as total_conversions, views+views_2 as video_views,
      ad_id, adset_id, campaign_id, channel as channel, creative_id, 0 as placement_id
from {{ref('src_ads_creative_facebook_all_data')}}

),

tiktok as (
SELECT  
      date, add_to_cart, clicks, 0 as comments, 0 as engagements, impressions,
      (skan_app_install + rt_installs) as installs, 0 as likes, 0 as link_clicks, 0 as post_click_conversions, 0 as postr_view_conversions,
       0 as posts, purchase, registrations, 0 as revenue, 0 as shares, spend, (conversions) as total_conversions, 
       video_views, ad_id, 0 as adset_id, campaign_id, channel, 0 as creative_id, 0 as placement_id 
FROM {{ ref('src_ads_tiktok_ads_all_data')}}
),

twitter as (
SELECT 
  date, 0 as add_to_cart, clicks, comments, engagements, impressions, 0 as installs, likes,
  url_clicks as link_clicks, 0 as post_click_conversions, 0 as post_view_conversions, 0 as posts, 
  0 as purchase, 0 as registrations, 0 as revenue, retweets as shares, spend,  0 as total_conversions,
  video_total_views as video_views, 0 as ad_id, 0 as  adset_id, campaign_id, channel, 0 as creative_id, 0 as placement_id
 FROM {{ref('src_promoted_tweets_twitter_all_data')}}
),

bing as (
SELECT  
  date, 0 as add_to_cart, clicks, 0 as comments, 0 as engagements, imps as impressions, 0 as installs, 0 as likes, 0 as link_links, 0 as post_click_conversions,
  0 as post_view_conversions, 0 as posts, 0 as purchase, 0 as registrations, revenue, 0 as shares, spend, conv as total_conversions, 0 as video_views, ad_id, adset_id, campaign_id, channel,
  0 as creative_id, 0 as placement_id
FROM {{ ref('src_ads_bing_all_data')}}
)

select * from twitter
union all
select * from tiktok
union all
select * from facebook
union all
select * from bing
