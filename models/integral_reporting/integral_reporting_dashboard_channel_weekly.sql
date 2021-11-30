WITH basis AS (
SELECT
vertaal.*, 
weekdate,
extract(isoweek from weekdate) as weeknr,
extract(isoyear from weekdate) as year
FROM 
`comscore-data-prod.ati.360_graden_rapportage_vertaaltabel_upload_20_21` as vertaal
--create one row per week of interest per title
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
),

--two filters to include data without a intekening: 
--either > 1000 views in one year, or at least 1 lineair new broadcast in year
--cleaning is only needed for streaming data in this view. 
--if there's no broadcast -it is not in the broadcast selection :) 

intekening_cleaning as 
(
select 
coalesce(ss.poms_series_id, tvb.poms_series_id) as poms_series_id,
coalesce(ss.year, tvb.year) as year, 
sum(ss.streaming_playcount_over_30s_broadcastsonly) as n_views_in_reportingyear, 
sum(tv_number_of_broadcasts) as n_broadcasts_in_year 
from
{{ ref('integral_reporting_vodstreaming') }} ss
full outer join {{ ref('integral_reporting_tvbroadcasts') }}  tvb 
on tvb.poms_series_id = ss.poms_series_id 
and tvb.year = ss.year and tvb.weeknr = ss.weeknr
GROUP BY
1,2

having sum(ss.streaming_playcount_over_30s_broadcastsonly) >= 1000 or sum(tvb.tv_number_of_broadcasts) >= 1
)

--union all the channels to get one line per channel but be able to stack and average metrics within a title/channel/date/week/whatever
--if a metric is missing in a channel: null it to allow smooth averages in reporting tools
--if you want to add a channel? create a new union

,union_of_channels as (


SELECT
basis.Naam,
basis.Net,
basis.Omroep,
basis.CCC,
basis.weekdate,
basis.weeknr,
basis.year,
if(basis.Naam is not null, 1,0) as has_intekening,
null as is_tv_week,
null as new_releases,
'facebook' as reporting_channel,
fb.fb_number_of_posts as n_items_per_week,
'posts' as media_item_type,


fb.fb_reach_per_week as weekly_reach_per_week,
null as daily_reach_per_week,
null as visits_per_week,
fb.fb_engagement_per_week as engagement_per_week,

null as views_per_week,
null as hours_watched,
null as video_kdh_per_week,
null as video_kdh_per_release

FROM basis
LEFT JOIN {{ ref('integral_reporting_facebook') }} as fb on basis.QL_FB_ID = fb.QL_FB_ID and basis.weekdate = fb.weekdate

UNION ALL

SELECT
basis.Naam,
basis.Net,
basis.Omroep,
basis.CCC,
basis.weekdate,
basis.weeknr,
basis.year,
if(basis.Naam is not null, 1,0) as has_intekening,
null as is_tv_week,
null as new_releases,
'instagram' as reporting_channel,
ig.ig_number_of_posts  as n_items_per_week,
'posts' as media_item_type,
ig.ig_reach_per_week  as weekly_reach_per_week,
null as daily_reach_per_week,
null as visits_per_week,
ig.ig_engagement_per_week  as engagement_per_week,

null as views_per_week,
null as hours_watched,
null as video_kdh_per_week,
null as video_kdh_per_release

FROM basis
LEFT JOIN {{ ref('integral_reporting_instagram') }} as ig on basis.QL_IG_ID = ig.QL_IG_ID and basis.weekdate = ig.weekdate

UNION ALL
--note: we take sites and app from the same base view for now, as they were visualised seperatly..
SELECT
basis.Naam,
basis.Net,
basis.Omroep,
basis.CCC,
basis.weekdate,
basis.weeknr,
basis.year,
if(basis.Naam is not null, 1,0) as has_intekening,
null as is_tv_week,
null as new_releases,
'sites' as reporting_channel,
null n_items_per_week,
null as media_item_type,
online.site_weekly_visitors   as weekly_reach_per_week,
online.site_daily_visitors   as daily_reach_per_week,
online.site_visits  as visits_per_week,

null as engagement_per_week,
null as views_per_week,
null as hours_watched,
null as video_kdh_per_week,
null as video_kdh_per_release

FROM basis
LEFT JOIN {{ ref('integral_reporting_sites_and_apps') }} as online on basis.ATI_Titel = online.ATI_Titel and basis.weekdate = online.weekdate

UNION ALL
--note: we take sites and app from the same base view for now, as they were visualised seperatly..
SELECT
basis.Naam,
basis.Net,
basis.Omroep,
basis.CCC,
basis.weekdate,
basis.weeknr,
basis.year,
if(basis.Naam is not null, 1,0) as has_intekening,
null as is_tv_week,
null as new_releases,
'apps' as reporting_channel,
null n_items_per_week,
null as media_item_type,
online.app_weekly_visitors    as weekly_reach_per_week,
online.app_daily_visitors    as daily_reach_per_week,
online.app_visits   as visits_per_week,

null as engagement_per_week,
null as views_per_week,
null as hours_watched,
null as video_kdh_per_week,
null as video_kdh_per_release

FROM basis
LEFT JOIN {{ ref('integral_reporting_sites_and_apps') }} as online on basis.ATI_Titel = online.ATI_Titel and basis.weekdate = online.weekdate


UNION ALL

SELECT
basis.Naam,
basis.Net,
basis.Omroep,
basis.CCC,
basis.weekdate,
basis.weeknr,
basis.year,
if(basis.Naam is not null, 1,0) as has_intekening,
null as is_tv_week,
null as new_releases,
'youtube' as reporting_channel,
yt.yt_number_of_videos   as n_items_per_week,
'videos' as media_item_type,
null  as weekly_reach_per_week,
null as daily_reach_per_week,
null as visits_per_week,
yt.yt_engagement_per_week  as engagement_per_week,

yt.yt_views_per_week  as views_per_week,
round(yt.yt_time_spent_per_week_min / 60,2) as hours_watched,
round(yt.yt_kdh_per_week,0) as video_kdh_per_week,
null as video_kdh_per_release
FROM basis
LEFT JOIN {{ ref('integral_reporting_youtube') }} as yt on basis.QL_YT_ID = yt.QL_YT_ID and basis.weekdate = yt.weekdate

UNION ALL

SELECT
coalesce(basis.Naam, POMS_series_title) as Naam,
basis.Net,
basis.Omroep,
basis.CCC,
coalesce(basis.weekdate, tv.weekdate) as weekdate,
coalesce(basis.weeknr, tv.weeknr) as weeknr,
coalesce(basis.year, tv.year) as year,
if(basis.Naam is not null, 1,0) as has_intekening,
tv.tv_broadcast_week as is_tv_week,
tv.tv_number_of_broadcasts as new_releases,
'lineair' as reporting_channel,
tv.tv_number_of_broadcasts    as n_items_per_week,
'videos' as media_item_type,
null  as weekly_reach_per_week,
null as daily_reach_per_week,
null as visits_per_week,
null as engagement_per_week,

null  as views_per_week,
round(tv.tv_time_spent_per_week_min  / 60,2) as hours_watched,
round(tv.tv_sum_kdh_per_week,0) as video_kdh_per_week,
round(tv.tv_sum_kdh_per_week / nullif(tv.tv_number_of_broadcasts,0),0) as video_kdh_per_release

FROM
{{ ref('integral_reporting_tvbroadcasts') }} as tv
LEFT JOIN
  basis on basis.Serie_mid = tv.poms_series_id  and basis.weekdate = tv.weekdate

UNION ALL

SELECT
coalesce(basis.Naam, stream.POMS_series_title) as Naam,
basis.Net,
basis.Omroep,
basis.CCC,
coalesce(basis.weekdate, stream.weekdate) as weekdate,
coalesce(basis.weeknr, stream.weeknr) as weeknr,
coalesce(basis.year, stream.year) as year,
max(if(basis.Naam is not null, 1,0)) as has_intekening,
null as is_tv_week,
null as nr_of_new_releases,
'VOD' as reporting_channel,
sum(stream.streaming_number_of_episodes) as n_items_per_week,
'videos' as media_item_type,
null  as weekly_reach_per_week,
null as daily_reach_per_week,
null as visits_per_week,
null as engagement_per_week,

sum(stream.streaming_playcount_over_30s)   as views_per_week,
round(sum(stream.streaming_time_spent_sec / 60  / 60),2) as hours_watched,
round(sum(stream.streaming_sum_kdh_per_week),0)  as video_kdh_per_week,
round(sum(streaming_kdh_for_new_released_episode) / nullif(sum(streaming_number_of_new_released_eps),0),0) as video_kdh_per_release
FROM  
{{ ref('integral_reporting_vodstreaming') }} as stream
inner join 
  intekening_cleaning scc on scc.POMS_series_id = stream.POMS_series_id and scc.year = stream.year
left join 
  basis on basis.Serie_mid = stream.POMS_series_id and basis.weekdate = stream.weekdate
GROUP BY
coalesce(basis.Naam, stream.POMS_series_title),
basis.Net,
basis.Omroep,
basis.CCC,
coalesce(basis.weekdate, stream.weekdate),
coalesce(basis.weeknr, stream.weeknr),
coalesce(basis.year, stream.year)
)

--finale select, calculate some metrics and max the tv_week to have this available on all channels in a title/week

select
  * except(is_tv_week, new_releases, has_intekening), 
  
  --youtube does not have reach, so we use the views per week. 
   round(engagement_per_week/nullif(IF(reporting_channel = 'youtube', views_per_week, weekly_reach_per_week),0),3) as engagement_ratio,
  --calculate dau/mau at the end for all relevant channels
   round(daily_reach_per_week / nullif(weekly_reach_per_week,0),2) as dau_mau_ratio,
  --because TV is dominant, we need a dummy for cool analysis purposes 
   max(is_tv_week) over (partition by weekdate, naam) as is_tv_week,
   SUM(IF(reporting_channel = 'lineair',new_releases,0)) over (partition by naam) as n_total_tv_broadcasts,
  
--for analytics over with/without intekening or data in the 'vertaaltabel'
   max(has_intekening) over (partition by naam) as has_intekening,
  --to bucketize within titles and avoid lineair clutter 
   case 
    when avg(IF(reporting_channel = 'lineair', video_kdh_per_release,0)) over (partition by naam) < 100000 then 'avg lineaire KDH < 100.000'
    when avg(IF(reporting_channel = 'lineair', video_kdh_per_release,0)) over (partition by naam) >= 4000000 then 'avg lineaire KDH > 4.000.000'
    when avg(IF(reporting_channel = 'lineair' and video_kdh_per_release is not null,1,0)) over (partition by naam) = 0 then 'no lineair KDH in this week'
   else "avg lineaire KDH >= 100.000 and < 4.0000.000"
  end as lineair_kdh_bucket
from
   union_of_channels