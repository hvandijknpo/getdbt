SELECT
  vertaaltabel.Target_YT_subscribers,
  vertaaltabel.Target_YT_views,
  vertaaltabel.QL_YT_ID, 
  weekdate, 
  EXTRACT(ISOWEEK FROM weekdate) AS weeknr, 
  EXTRACT(ISOYEAR FROM weekdate) AS year,
  yt.totalSubscribers as yt_total_subscribers,
  yt.totalSubscribersChange as yt_total_subscribers_change,
  yt.totalVideos as yt_number_of_videos,
  yt.views as yt_views_per_week,
  yt.estimatedminuteswatched as yt_time_spent_per_week_min,
  yt.totalengagement as yt_engagement_per_week,
  yt.averageViewPercentage as yt_view_percentage,
  yt.averageViewDuration as yt_view_duration,
  (yt.averageViewPercentage/100) * yt.views as yt_kdh_per_week

FROM {{ ref('360_graden_rapportage_vertaaltabel_upload_20_21') }}  as vertaaltabel
--create one row per week of interest per title
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
--left join the youtube data to also get a record per week where there's no data/no broadcast
LEFT JOIN {{ ref('quintly_youtube_allchannels_weekly') }} yt on
  vertaaltabel.QL_YT_ID = yt.profileId 
  AND EXTRACT(ISOYEAR FROM intervalBegin) = EXTRACT(ISOYEAR FROM weekdate)
  AND EXTRACT(ISOWEEK FROM intervalBegin) = EXTRACT(ISOWEEK FROM weekdate)

WHERE vertaaltabel.Naam IS NOT NULL