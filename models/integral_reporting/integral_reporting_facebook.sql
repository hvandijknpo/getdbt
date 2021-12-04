SELECT
  vertaaltabel.Target_FB_pagelikes,
  vertaaltabel.Target_FB_reachperpost,
  vertaaltabel.QL_FB_ID, 
  weekdate, 
  EXTRACT(ISOWEEK FROM weekdate) AS weeknr, 
  EXTRACT(ISOYEAR FROM weekdate) AS year,
  fb.fans as fb_fans,
  fb.fansChange as fb_fans_change,
  fb.ownPosts as fb_number_of_posts,
  fb.pageImpressionsUnique as fb_reach_per_week,
  fb.ownPostsEngagement as fb_engagement_per_week

FROM {{ ref('360_graden_rapportage_vertaaltabel_upload_20_21') }}  as vertaaltabel
--create one row per week of interest per title
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
--left join the youtube data to also get a record per week where there's no data/no broadcast
LEFT JOIN {{ ref('quintly_facebook_pages_weekly') }} fb on
  vertaaltabel.QL_FB_ID = fb.profileId 
  AND EXTRACT(ISOYEAR FROM intervalBegin) = EXTRACT(ISOYEAR FROM weekdate)
  AND EXTRACT(ISOWEEK FROM intervalBegin) = EXTRACT(ISOWEEK FROM weekdate)

WHERE vertaaltabel.Naam IS NOT NULL

