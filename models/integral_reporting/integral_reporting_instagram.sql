SELECT
  vertaaltabel.Target_IG_followers,
  vertaaltabel.Target_IG_reachperpost,
  vertaaltabel.QL_IG_ID, 
  weekdate, 
  EXTRACT(ISOWEEK FROM weekdate) AS weeknr, 
  EXTRACT(ISOYEAR FROM weekdate) AS year,
  ig.followers as ig_followers,
  ig.followersChange as ig_followers_change,
  ig.posts as ig_number_of_posts,
  ig.postschange as ig_number_of_posts_change,
  ig.reach as ig_reach_per_week,
  ig.totalengagement as ig_engagement_per_week

FROM {{ ref('360_graden_rapportage_vertaaltabel_upload_20_21') }}  as vertaaltabel
--create one row per week of interest per title
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
--left join the youtube data to also get a record per week where there's no data/no broadcast
LEFT JOIN {{ ref('quintly_instagram_pages_weekly') }} ig on
  vertaaltabel.QL_IG_ID = ig.profileId 
  AND EXTRACT(ISOYEAR FROM intervalBegin) = EXTRACT(ISOYEAR FROM weekdate)
  AND EXTRACT(ISOWEEK FROM intervalBegin) = EXTRACT(ISOWEEK FROM weekdate)

WHERE vertaaltabel.Naam IS NOT NULL
