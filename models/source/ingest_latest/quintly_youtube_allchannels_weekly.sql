--declare source table
with quintly_youtube_allchannels_weekly 
as (select * from {{ source('quintly_youtube_allchannels_weekly', 'v1') }})



SELECT
     * EXCEPT(partitionDate)
FROM 
    quintly_youtube_allchannels_weekly
    --`npo-data-hub.quintly_youtube_allchannels_weekly.v1`
WHERE 
    partitionDate = (SELECT MAX(partitionDate) FROM quintly_youtube_allchannels_weekly)