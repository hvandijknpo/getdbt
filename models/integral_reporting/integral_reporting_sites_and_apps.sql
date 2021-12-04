-- combining ati plussites with subsites
WITH ati AS ( 
SELECT
level_2,
platform,
weekdate,
weeknum,
year,
weekly_visitors,
daily_visitors,
visits
FROM `npo-publieksonderzoek-datamart.atinternet_smarttag_pages_weekly.v2` 
UNION ALL
Select 
concat(level_2, " - ", programme) as level_2,
platform,
weekdate,
weeknum,
year,
weekly_visitors,
daily_visitors,
visits
FROM `npo-publieksonderzoek-datamart.atinternet_smarttag_pages_programmes_weekly.v2` 
)

SELECT
  vertaaltabel.Target_AT_app,
  vertaaltabel.Target_AT_site,
  vertaaltabel.ATI_Titel, 
  weekdate, 
  EXTRACT(ISOWEEK FROM weekdate) AS weeknr, 
  EXTRACT(ISOYEAR FROM weekdate) AS year,
  SUM(IF(ati.platform LIKE 'app', ati.weekly_visitors, null)) app_weekly_visitors,
  SUM(IF(ati.platform LIKE 'site', ati.weekly_visitors, null)) site_weekly_visitors,
  SUM(IF(ati.platform LIKE 'app', ati.daily_visitors, null)) app_daily_visitors,
  SUM(IF(ati.platform LIKE 'site', ati.daily_visitors, null)) site_daily_visitors,
  SUM(IF(ati.platform LIKE 'app', ati.visits, null)) app_visits,
  SUM(IF(ati.platform LIKE 'site', ati.visits, null)) site_visits
FROM {{ ref('360_graden_rapportage_vertaaltabel_upload_20_21') }}  as vertaaltabel
--create one row per week of interest per title
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY('2018-12-31', CURRENT_DATE(), INTERVAL 1 WEEK)) as weekdate
--left join the ati data to also get a record per week where there's no data/no broadcast
LEFT JOIN ati on
  vertaaltabel.ATI_Titel = ati.level_2  
  AND ati.year = EXTRACT(ISOYEAR FROM weekdate)
  AND ati.weeknum = EXTRACT(ISOWEEK FROM weekdate)

WHERE vertaaltabel.Naam IS NOT NULL
GROUP BY 1,2,3,4,5,6