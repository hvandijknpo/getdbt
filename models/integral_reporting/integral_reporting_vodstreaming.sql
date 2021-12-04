WITH new_tv_eps AS (
--get new released broadcasts with their date, and their corresponding season start_enddate
SELECT
title,
date as first_broadcast_date,
tveps.beginTimeCET as start_eps,
mediaId,
poms_info.season_id 
channel,
date_diff(endTimeCET, beginTimeCET, MINUTE) as ep_duration_min, 
MIN(DATE) over (partition by season_id) as first_known_eps_date_season,
max(date) over (partition by season_id) as last_known_eps_date_season
FROM
{{ ref('advantedge_tv_viewer_density_per_show_daily') }} tveps
left join {{ ref('poms_flattened') }}  as poms_info on poms_info.mid = tveps.mediaId 
WHERE regexp_contains(Channel, 'NPO') and RepeatType = 'FIRST' and audience = '6+' and universe = 'Nat[SKO]' and extract(isoyear from date) >= 2019 
),

first_streaming_days as 

(
select
  evt_mid,
  min(evt_date) as first_stream_day
from
(
SELECT
evt_mid,
evt_date, 
SUM(evt_play_count_over_30s) as streaming_playcount_over_30s,
FROM
{{ ref('v4') }} stream_eps 
where mtd_type = 'BROADCAST'
group by evt_mid, evt_date
)
where streaming_playcount_over_30s >= 100

group by evt_mid
),


new_releases as 

(
select distinct
poms.episode_id as mediaid, 
poms.series_ref,
poms.series_title,
poms.episode_type,
poms.season_ref,
poms.index as eps_nr,
poms.season_index as season_nr,
--logic: if there's an advantedge date; use that that. if there is NO advantedge date: check if the first broadcast scheduled lies within 90 days of the first online streaming day with >100 views.
--if so: use the scheduled/fake first broadcast date. if not: the first streaming day is used as the day of a relase - depicted as an online only release
--note: scheduled date is labelled 'start lineair first broadcast date' in POMS

coalesce(first_broadcast_date, if(date_diff(date(start_linear_first_broadcast),first_stream_day,  day) >= 90, first_stream_day, date(start_linear_first_broadcast))) as first_broadcast_date,
if(first_broadcast_date is null,0,1) as has_had_linear_release,
if(coalesce(first_broadcast_date, date(start_linear_first_broadcast)) is null,0,1) as has_scheduled_lineair_release,
min(coalesce(first_broadcast_date, if(date_diff(date(start_linear_first_broadcast),first_stream_day,  day) >= 90, first_stream_day, date(start_linear_first_broadcast)))) over (partition by season_ref) as first_broadcast_season,
max(coalesce(first_broadcast_date, if(date_diff(date(start_linear_first_broadcast),first_stream_day,  day) >= 90, first_stream_day, date(start_linear_first_broadcast)))) over (partition by season_ref) as last_known_broadcast_season,
from
 {{ ref('lookerdimension_poms_episodes_materialized') }}  poms 
left join new_tv_eps nte on poms.episode_id = nte.mediaId
left join first_streaming_days fsd on fsd.evt_mid = poms.episode_id
where episode_type = 'BROADCAST'
)
,streaming_info as  

(
--new releases --> ONLY their KDH and match it to the release-week
SELECT
evt_mid,
poms.series_ref,
--mtd_title_main as series_title,
poms.series_title,
evt_programme,
EXTRACT(ISOWEEK FROM first_broadcast_date) AS evt_weeknr, 
EXTRACT(ISOYEAR FROM first_broadcast_date) AS evt_year,
AVG(mtd_duration_in_sec) as streaming_duration_sec,
null as streaming_playcount_over_30s,
null as streaming_playcount_over_30s_broadcastsonly,
null as streaming_time_spent_sec,
SUM(case when mtd_type = 'BROADCAST' AND  date_diff( evt_date, last_known_broadcast_season, day) <= 28 then evt_playback_time_total_in_sec else 0 end) / nullif(
MAX(case when mtd_type = 'BROADCAST' AND  date_diff( evt_date, last_known_broadcast_season, day) <= 28 then mtd_duration_in_sec else 0 end),0)
as streaming_kdh_for_new_released_episode,
max(case when mtd_type = 'BROADCAST' AND date_diff( evt_date, last_known_broadcast_season, day) <= 28 then evt_mid else null end) as mid_counts_for_new_release,
null as streaming_kdh_per_episode,
max(case when has_had_linear_release = 1 then 1 else 0 end) as has_had_linear_release,
max(case when has_had_linear_release = 0 and has_scheduled_lineair_release = 1 then 1 else 0 end) as has_scheduled_lineair_release,
max(case when has_had_linear_release = 0 and has_scheduled_lineair_release = 0 then 1 else 0 end) as vod_only_release,


FROM
{{ ref('v4') }} stream_eps
left join new_releases on new_releases.mediaId = stream_eps.evt_mid
left join {{ ref('lookerdimension_poms_episodes_materialized') }}  poms on poms.episode_id = stream_eps.evt_mid
GROUP BY 1,2,3,4,5,6

UNION ALL
--repeat logic to get hours watched and views, but put this in the week streamed and do NOT take the KDH
SELECT
evt_mid,
poms.series_ref,
--mtd_title_main as series_title,
poms.series_title,
evt_programme,
EXTRACT(ISOWEEK FROM evt_date) AS evt_weeknr, 
EXTRACT(ISOYEAR FROM evt_date) AS evt_year,
AVG(mtd_duration_in_sec) as streaming_duration_sec,
SUM(evt_play_count_over_30s) as streaming_playcount_over_30s,
SUM(case when mtd_type = 'BROADCAST' then evt_play_count_over_30s else 0 end) as streaming_playcount_over_30s_broadcastsonly,
SUM(evt_playback_time_total_in_sec) as streaming_time_spent_sec,

null as streaming_kdh_for_new_released_episode,
null as mid_counts_for_new_release,
SUM(evt_playback_time_total_in_sec) / nullif(AVG(mtd_duration_in_sec), 0) as streaming_kdh_per_episode,
null as has_had_linear_release,
null as has_scheduled_lineair_release,
null as vod_only_release
FROM
{{ ref('v4') }} stream_eps
left join {{ ref('lookerdimension_poms_episodes_materialized') }} poms on poms.episode_id = stream_eps.evt_mid
GROUP BY 1,2,3,4,5,6
)

--combine the data and aggregate to vertaaltabel_week level

SELECT
  coalesce(vertaaltabel.Stream_Titel, series_title) as Stream_join_title,
  series_title as POMS_series_title,
  series_ref as POMS_series_id,
  if(vertaaltabel.Stream_Titel is null,0,1) as has_intekening,
  weekdate, 
  EXTRACT(ISOWEEK FROM weekdate) AS weeknr, 
  EXTRACT(ISOYEAR FROM weekdate) AS year,
  COUNT(distinct evt_mid) AS streaming_number_of_episodes,
  SUM(str.streaming_duration_sec) as streaming_duration_sec,
  SUM(str.streaming_playcount_over_30s) as streaming_playcount_over_30s,
  SUM(str.streaming_playcount_over_30s_broadcastsonly) as streaming_playcount_over_30s_broadcastsonly,
  SUM(str.streaming_time_spent_sec) as streaming_time_spent_sec,
  SUM(str.streaming_kdh_per_episode) as streaming_sum_kdh_per_week,
  SUM(str.streaming_kdh_for_new_released_episode) as streaming_kdh_for_new_released_episode,
  count(distinct mid_counts_for_new_release) as streaming_number_of_new_released_eps,
  max(has_had_linear_release) as week_with_new_lineair_release,
  max(case when has_had_linear_release = 1 or has_scheduled_lineair_release = 1 or vod_only_release = 1 then 1 else 0 end) as week_with_new_release
FROM 
--create one row per week of interest per title
UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
left join streaming_info str on evt_year = EXTRACT(ISOYEAR FROM weekdate)
  AND evt_weeknr = EXTRACT(ISOWEEK FROM weekdate)

LEFT JOIN {{ ref('360_graden_rapportage_vertaaltabel_upload_20_21') }} as vertaaltabel on
  vertaaltabel.Serie_mid = str.series_ref


GROUP BY 1,2,3,4,5
order by weekdate desc