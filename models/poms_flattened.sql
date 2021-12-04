WITH
exploded_references AS (
  SELECT
    media.id AS mid,
    media.type AS `type`,
    media.sort_date AS sort_date,
    media.duration AS duration,
    media.age_rating AS age_rating,
    broadcasters.id AS broadcaster_id,
    broadcaster_offset,
    CASE WHEN broadcasters.id = "NOS" THEN TRUE ELSE FALSE END AS is_nos_content,
    MAX(CASE WHEN STARTS_WITH(unnested_genres.id, "3.0.1.1") THEN TRUE ELSE FALSE END ) AS is_youth_genre,
    MAX(CASE WHEN schedule_events.net = "ZAPP" THEN TRUE ELSE FALSE END ) AS is_zapp_net,
    MAX(CASE WHEN schedule_events.net = "ZAPPE" THEN TRUE ELSE FALSE END ) AS is_zappelin_net,
    MAX(CASE WHEN episode_of.type = 'SERIES' THEN episode_of.mid_ref ELSE NULL END ) AS episode_of_series,
    MAX(CASE WHEN episode_of.type = 'SEASON' THEN episode_of.mid_ref ELSE NULL END ) AS episode_of_season,
    MAX(CASE WHEN descendant_of.type = 'SERIES' THEN descendant_of.mid_ref ELSE NULL END ) AS descendant_of_series,
    MAX(CASE WHEN descendant_of.type = 'SEASON' THEN descendant_of.mid_ref ELSE NULL END ) AS descendant_of_season,
    MAX(CASE WHEN member_of.type = 'SERIES' THEN member_of.mid_ref ELSE NULL END ) AS member_of_series,
    MAX(CASE WHEN member_of.type = 'SEASON' THEN member_of.mid_ref ELSE NULL END ) AS member_of_season
  FROM
{{ ref('audiovisual_metadata_poms_metadata_v1') }} AS media
  LEFT JOIN UNNEST (episode_of) AS episode_of
  LEFT JOIN UNNEST (descendant_of) AS descendant_of
  LEFT JOIN UNNEST (member_of) AS member_of
  LEFT JOIN UNNEST (genres) AS unnested_genres
  LEFT JOIN UNNEST (schedule_events) AS schedule_events
  LEFT JOIN UNNEST (broadcasters) AS broadcasters WITH OFFSET AS broadcaster_offset
  -- See https://jira.publiekeomroep.nl/browse/MIT-939 for motivation for filtering below broadcasters
  WHERE (broadcasters.id NOT IN ('PP','RVD','RNW','SOCU','BVN','MTNL','EXT') OR broadcasters.id IS NULL)
  GROUP BY
    mid,
    `type`,
    age_rating,
    sort_date,
    duration,
    broadcaster_id,
    broadcaster_offset
),

define_columns AS (
  SELECT
    mid,
    `type`,
    CASE
      WHEN `type` = 'BROADCAST' THEN COALESCE(episode_of_series,  member_of_series,  descendant_of_series)
      WHEN `type` = 'SEASON' THEN member_of_series
      WHEN `type` = 'SEGMENT' THEN descendant_of_series
    END
    AS series_ref,
    CASE
      WHEN `type` = 'BROADCAST' THEN COALESCE(member_of_season,  descendant_of_season,  episode_of_season)
      WHEN `type` = 'SEGMENT' THEN descendant_of_season
    END
    AS season_ref,
    duration,
    sort_date,
    age_rating,
    is_youth_genre,
    is_zapp_net,
    is_zappelin_net,
    broadcaster_id,
    is_nos_content,
    -- Determine the position of broadcaster in the array, main broadcaster is thus considered first broadcaster after
    -- filtering of regional broadcasters.
    ROW_NUMBER() OVER (PARTITION BY mid ORDER BY broadcaster_offset ASC) AS broadcaster_row_number
  FROM
    exploded_references
)

SELECT
  mid,
  `type`,
  CASE WHEN (series_ref IS NULL OR series_ref = '') THEN mid ELSE series_ref END AS series_id,
  CASE WHEN (season_ref IS NULL OR season_ref = '') THEN mid ELSE season_ref END AS season_id,
  -- Content age classification rules by courtesy of Camiel:
  -- Als je <6 bent => IF (channel/net == ZAPPE) OR ((nicam==ALL) AND genre==jeugd)
  -- Als je <9 bent => IF (channel/net == ZAPPE) OR ((nicam==ALL||6) AND genre==jeugd)
  -- Als je <12 bent => IF (channel/net == ZAPP || ZAPPE) OR ((nicam==ALL||6||9) AND genre==jeugd)
  -- nicam == age_rating
  -- genre should start with "3.0.1.1" to be classified as "jeugd"
  -- check schedule_events.net, since schedule_events.channels never contain ZAPP or ZAPPE..
  CASE WHEN (is_zappelin_net OR (age_rating = "ALL" AND is_youth_genre)) THEN 6
       WHEN (is_zappelin_net OR (age_rating IN ("ALL", "6") AND is_youth_genre)) THEN 9
       WHEN (is_zapp_net OR is_zappelin_net OR (age_rating IN ("ALL", "6", "9") AND is_youth_genre)) THEN 12
  END AS min_age_classification,
  sort_date,
  duration,
  broadcaster_id AS first_broadcaster,
  is_nos_content
FROM
  define_columns
WHERE
  broadcaster_row_number = 1