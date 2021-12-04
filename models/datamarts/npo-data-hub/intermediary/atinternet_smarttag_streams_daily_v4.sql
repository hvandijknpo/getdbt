{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 1 day)',
    'date_sub(current_date, interval 2 day)',
    'date_sub(current_date, interval 3 day)',
    'date_sub(current_date, interval 4 day)',
    'date_sub(current_date, interval 5 day)',   
    'date_sub(current_date, interval 6 day)',
    'date_sub(current_date, interval 7 day)',
    'date_sub(current_date, interval 8 day)'
    
] %}


{{ config(
    schema='atinternet_smarttag_streams_daily',
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'evt_date',
    partitions = partitions_to_replace
  )
}}

-- Events from AdvantEdge, unpivot to have a row per minute (which will be joined on AT Internet events) to match livestreaming on the broadcasted show
-- All broadcasted shows are included, including reruns
WITH adv_events AS (
    SELECT
        date as adv_date,
        --NOTE: CET AS AT INTERNET IS ALSO IN CET!
        beginTimeCET as adv_beginTimeCET,
        endTimeCET as adv_endTimeCET,
        min_interval as adv_minute,       
        title as adv_title,
        channel as adv_channel,
        mediaId as adv_mid,
    FROM {{ ref('advantedge_tv_viewer_density_per_show_daily_v1') }}
    INNER JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP(beginTimeCET), TIMESTAMP(endTimeCET), INTERVAL 1 MINUTE)) AS min_interval
    WHERE
        REGEXP_CONTAINS(channel, "NPO")
    GROUP BY 1,2,3,4,5,6,7
)
,
raw_events as (

SELECT
    -- Do a (sub)query to make it easier to work with the SPLIT() columns.
    d_rm_playid,
    concat(d_visit_id, d_uv_id) as unique_user_id,
    d_date_hour_event,
    d_rm_action,
    d_rm_l2,
    d_rm_playback_time,
    SPLIT(d_rm_content, '_||_') AS content,
    adv_mid as matched_livestream_id,
    SPLIT(d_rm_theme1, '_||_') AS theme1,
    -- There is bug that causes d_rm_theme2 to sometimes contain a value
    -- of the form /d{2}:/d{2}:/d{2}, for example 00:00:01.
    -- The regexp filters those out.
    IF(REGEXP_CONTAINS(d_rm_theme2, '_/|/|_'), SPLIT(d_rm_theme2, '_||_'), NULL) AS theme2,
    SPLIT(d_rm_theme3, '_||_') AS theme3,
    IF(adv_mid is not null, 'livetvzender - streamID_matched', 'livetvzender - no_streamID_to_match') as stream_match_type,
     --to avoid dupes we create a rownumber to match to the nearest started show. 
    --the lowest granularity of this table should be unique_play|eventtimestamp|eventtype|timelogged|content ID       
    row_number() over (partition by d_rm_playid, concat(d_visit_id, d_uv_id), d_date_hour_event, d_rm_action, d_rm_playback_time order by timestamp_diff(d_date_hour_event, timestamp(adv_beginTimeCET), second) asc) as dedup    
 FROM {{ ref('media_events') }} as ati_events 
 --left join because we also report on non-matched-livestreams as they are a part of the total hours streamed. 
 --this may include commercials e.g.
--NOTE: WE JOIN ON CET AS AT-INTERNET EVENTS ARE IN CET!
 inner JOIN {{ ref('live_stream_name_mapping_v1') }} as channel_mapping ON channel_mapping.channel_id = NULLIF(SPLIT(ati_events.d_rm_content, '_||_')[SAFE_OFFSET(1)], '')
 LEFT JOIN adv_events ON channel_mapping.channel = adv_events.adv_channel and TIMESTAMP_TRUNC(d_date_hour_event, MINUTE) = adv_minute
        -- Do not count animation events.
        WHERE d_rm_type <> 'Animations'
        --only onclude live streaming here, and we only match TV for now
        AND d_rm_theme1 = "livetvzender" 
        AND date(d_date_hour_event) in ({{ partitions_to_replace | join(',') }})

 UNION ALL
  SELECT
    -- Do a (sub)query to make it easier to work with the SPLIT() columns.
    d_rm_playid,
    concat(d_visit_id, d_uv_id) as unique_user_id,
    d_date_hour_event,
    d_rm_action,
    d_rm_l2,
    d_rm_playback_time,
    SPLIT(d_rm_content, '_||_') AS content,
    NULL AS matched_livestream_id, -- only relevant for livestreams TV
    SPLIT(d_rm_theme1, '_||_') AS theme1,
    -- There is bug that causes d_rm_theme2 to sometimes contain a value
    -- of the form /d{2}:/d{2}:/d{2}, for example 00:00:01.
    -- The regexp filters those out.
    IF(REGEXP_CONTAINS(d_rm_theme2, '_/|/|_'), SPLIT(d_rm_theme2, '_||_'), NULL) AS theme2,
    SPLIT(d_rm_theme3, '_||_') AS theme3,
    'regular streamid' as stream_match_type,
    1 as dedup --fake a dedup for the other streams
 FROM {{ ref('media_events') }}
        -- Do not count animation events.
        WHERE d_rm_type <> 'Animations'
        -- Only process events within [begin, end) range.
        AND (d_rm_theme1 is null or d_rm_theme1 <> "livetvzender") 
        AND date(d_date_hour_event) in ({{ partitions_to_replace | join(',') }})
),

-- The transformed media events table on stream-level. Need to aggregate events on playid-level to calculate the playback_time_total_in_sec. In next query this will be used to check time_spent per playid.
agg_events_per_stream AS (
  SELECT
    d_rm_playid, 
    unique_user_id,
    DATE(d_date_hour_event) AS evt_date,
    -- Split some of the values into seperate columns
    -- and do some basic data cleaning so that all 'null' values
    -- are stored as actual NULLs.
    NULLIF(content[SAFE_OFFSET(1)], '') AS evt_base_mid,
    stream_match_type,
    coalesce(matched_livestream_id, NULLIF(content[SAFE_OFFSET(1)], '')) as evt_mid, 
    NULLIF(content[SAFE_OFFSET(0)], '') AS evt_media_name,
    NULLIF(NULLIF(theme1[SAFE_OFFSET(0)], ''), 'null') AS evt_stream_type,
    NULLIF(theme2[SAFE_OFFSET(0)], 'null') AS evt_programme,
    NULLIF(theme2[SAFE_OFFSET(1)], 'null') AS evt_broadcaster,
    theme2[SAFE_OFFSET(2)] AS evt_programme_podcast,
    -- These values do not seem to need data cleaning.
    theme3[SAFE_OFFSET(0)] AS evt_player_platform,
    theme3[SAFE_OFFSET(1)] AS evt_player_version,
    d_rm_l2 as evt_brand,
    -- Cumulative playback time.
    SUM(d_rm_playback_time) as evt_playback_time_total_in_sec,
   -- Count the number of distinct d_rm_playid's within a row. 
    --For livestreaming matched to a mediafile this means we include ALL unique d_rm_playid's and their plays 
    --we do not force a play for starts within a commercial/non-matched-item, but if it happens - we log it
    --for the regular items we only include PLAY actions. 
     --SO the unique count of d_rm_playid will differ from AT internet webversion 
     --but we don't care. Or we do...we'll see..
    COUNT(DISTINCT IF(d_rm_action = 'Play' OR stream_match_type = 'livetvzender - streamID_matched', d_rm_playid,null)) as evt_play_count_total
FROM raw_events
WHERE dedup = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14
),

-- This is where we only count playid's with time_spent of 30 sec. and up. This one is needed as a metric. 
agg_events_per_date AS (
  SELECT
    -- Group by date.
    evt_date,
    evt_mid,
    evt_base_mid,
    evt_media_name,
    stream_match_type,
    evt_stream_type,
    evt_programme,
    evt_broadcaster,
    evt_programme_podcast,
    evt_player_platform,
    evt_player_version,
    evt_brand,
    count(distinct unique_user_id) as n_unique_userids,
    SUM(evt_playback_time_total_in_sec) AS evt_playback_time_total_in_sec,
    SUM(evt_play_count_total) as evt_play_count_total,
    SUM(IF(evt_playback_time_total_in_sec >= 30, evt_play_count_total,0)) AS evt_play_count_over_30s
FROM agg_events_per_stream
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12
),

-- Number the seasons and series and make them indexable.
-- We need this to be able to select the first season and series later on.
metadata_descendants AS (
  SELECT
    md.id,
    d.type,
    d.mid_ref,
    -- Number repetitions of (md.id, d.type).
    ROW_NUMBER() OVER(PARTITION BY md.id, d.type ORDER BY md.id, d.type, d.mid_ref) AS index,
  FROM {{ ref('audiovisual_metadata_poms_metadata_v1') }} AS md
  CROSS JOIN UNNEST(descendant_of) AS d
  WHERE d.type in ('SEASON', 'SERIES')
  ORDER BY 1, 2, 3
),
-- The transformed metadata table.
metadata AS (
    SELECT
        md.id,
        DIV(md.duration, 1000) AS mtd_duration_in_sec,
        md.sort_date AS mtd_sort_date,
        md.type AS mtd_type,
        -- This is no definite order to genres so we just pick the first one.
        genres[SAFE_OFFSET(0)].terms[SAFE_OFFSET(0)] as mtd_genre_main,
        genres[SAFE_OFFSET(0)].terms[SAFE_OFFSET(1)] as mtd_genre_sub,
        titles[SAFE_OFFSET(0)].value AS mtd_title_main,
        titles[SAFE_OFFSET(1)].value AS mtd_title_sub,
        season.mid_ref AS mtd_season_mid,
        series.mid_ref AS mtd_series_mid,
        episode.index AS mtd_episode_of_index,
        channels.channel AS mtd_channel,
        broadcasters.broadcasters AS mtd_broadcasters,
    FROM {{ ref('audiovisual_metadata_poms_metadata_v1') }} AS md
    -- Get the season and series.
    LEFT JOIN metadata_descendants AS season
        ON (season.id = md.id AND season.type='SEASON' AND season.index = 1)
    LEFT JOIN metadata_descendants AS series
        ON (series.id = md.id AND series.type='SERIES' AND series.index = 1)

    -- It is not possible to do an outer join with UNNEST.
    -- So we have to do (inner) CROSS JOIN UNNEST in a subquery and then LEFT JOIN on it.
    -- Otherwise rows will be filtered out instead of producing NULL values.

    -- Picking the highest episode is kind of arbitrary but we need a single value.
    LEFT JOIN (
        SELECT
            md.id,
            MAX(IF(e.type = 'SEASON', e.index, NULL)) AS index,
        FROM {{ ref('audiovisual_metadata_poms_metadata_v1') }}  AS md
        CROSS JOIN UNNEST(md.episode_of) AS e
        GROUP BY 1
    ) AS episode ON (episode.id = md.id)

    -- Aggregate broadcaster values.
    LEFT JOIN (
        SELECT
            md.id,
            STRING_AGG(b.value, ',' ORDER BY b.value) AS broadcasters,
        FROM `npo-data-hub.audiovisual_metadata_v1.poms_metadata_v1` AS md
        CROSS JOIN UNNEST(md.broadcasters) AS b
        GROUP BY 1
    ) AS broadcasters ON (broadcasters.id = md.id)

    -- Number the channels and make them indexable.
    LEFT JOIN (
      SELECT
        md.id,
        s.channel AS channel,
        ROW_NUMBER() OVER(PARTITION BY md.id ORDER BY md.id, s.channel) AS index
      FROM {{ ref('audiovisual_metadata_poms_metadata_v1') }} AS md
      CROSS JOIN UNNEST(md.schedule_events) AS s
      -- We only care about NED1,2,3.
      WHERE s.channel in ('NED1', 'NED2', 'NED3')
      GROUP BY 1, 2
      ORDER BY 1, 2
    ) AS channels ON (channels.id = md.id AND channels.index = 1)
)
-- Finally, join it all together.
SELECT
  agg_events_per_date.*,
  metadata.* EXCEPT(id)
FROM agg_events_per_date
LEFT JOIN metadata ON (metadata.id = agg_events_per_date.evt_mid)