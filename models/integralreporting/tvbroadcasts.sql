WITH tv_ep AS (
SELECT
title,
date,
mediaId,
poms.series_ref as poms_series_id,
poms.series_title as poms_series_title,
channel,
date_diff(min(endTimeCET), min(beginTimeCET), MINUTE) as ep_duration_min, 
sum(kdh) as kdh,
date_diff(min(endTimeCET), min(beginTimeCET), MINUTE) * sum(kdh) as time_spent
FROM
`npo-publieksonderzoek-datamart.advantedge_tv_viewer_density_per_show_daily.v1` tvbroadcasts
left join `npo-data-hub.looker.poms_episodes_materialized` poms on poms.episode_id = tvbroadcasts.mediaId 
WHERE regexp_contains(Channel, 'NPO') and RepeatType = 'FIRST' and audience = '6+' and universe = 'Nat[SKO]' and extract(isoyear from date) >= 2019 
GROUP BY 1,2,3,4,5,6
),

tv_title AS (
SELECT
Title,
poms_series_id,
poms_series_title,
EXTRACT(ISOWEEK from date) as weeknr,
EXTRACT(ISOYEAR from date) as year,
COUNT(DISTINCT concat(mediaId, " - ", date)) as tv_number_of_broadcasts,
SUM(ep_duration_min) as tv_duration_min,
SUM(kdh) as tv_sum_kdh_per_week,
SUM(time_spent) as tv_time_spent_per_week_min
FROM
tv_ep
GROUP BY 1,2,3,4,5
)

 
SELECT
vertaal.Naam,
vertaal.Net,
vertaal.Omroep,
vertaal.CCC,
weekdate,
tv_title.weeknr,
tv_title.year,
tv_title.Title as AdE_Titel,
CASE WHEN tv_title.Title IS NOT NULL THEN 1 ELSE 0 END as tv_broadcast_week,
tv_title.poms_series_id,
tv_title.poms_series_title,
tv_number_of_broadcasts,
tv_duration_min,
tv_sum_kdh_per_week,
tv_time_spent_per_week_min,
FROM
  UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
left join 
  tv_title on EXTRACT(ISOWEEK FROM weekdate) = tv_title.weeknr and tv_title.year = EXTRACT(ISOYEAR FROM weekdate)
left join
  `comscore-data-prod.ati.360_graden_rapportage_vertaaltabel_upload_20_21` as vertaal on vertaal.AdE_Titel = tv_title.Title