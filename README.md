# Spotify_ETL

Ongoing Project - ETL Process

## Step 0: Data Ingestion / Process / Storage
- Pull song/album/artists data using Spotify API
- Process/Clean Data
- Load data into s3

## Step 1: Python Script Scheduled Execution

- Use AWS Lambds to run python script daily at 5pm

## Step 2: Query S3 data with AWS Athena
- Use AWS Athena to query S3 data
-   CREATE EXTERNAL TABLE IF NOT EXISTS `test`.`artist` (
  `artist_id` string,
  `artist_name` string,
  `artist_type` string
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://spotifyrecord/artists/'
TBLPROPERTIES ('has_encrypted_data'='false',"skip.header.line.count"="1");
- CREATE EXTERNAL TABLE IF NOT EXISTS `test`.`song` (
  `song_id` string,
  `song_name` string,
  `song_duration` int,
  `song_popularity` int,
  `song_played_at` string,
  `album_id` string,
  `artist_id` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://spotifyrecord/songs/'
TBLPROPERTIES ('has_encrypted_data'='false',"skip.header.line.count"="1");
- CREATE EXTERNAL TABLE IF NOT EXISTS `spotify_analysis`.`time` (
  `song_played_at` string,
  `song_played_date` string,
  `month` int,
  `day` int,
  `week_number` int,
  `weekday` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '"',
  'field.delim' = ','
) LOCATION 's3://spotifyrecord/time/'
TBLPROPERTIES ('has_encrypted_data'='false',"skip.header.line.count"="1");
- CREATE EXTERNAL TABLE IF NOT EXISTS `spotify_analysis`.`album` (
  `album_id` string,
  `album_name` string,
  `album_type` string,
  `album_release_date` string,
  `album_total_tracks` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://spotifyrecord/albums/'
TBLPROPERTIES ('has_encrypted_data'='false',"skip.header.line.count"="1");
- 
create table final as 
select distinct 
song_played_at ,song_id,song_name, song_duration, song_popularity  ,   
album_id ,album_name,album_type,album_total_tracks  ,album_release_date  ,
artist_id , artist_name, artist_type  , song_played_date  ,  
month , day, week_number,weekday   
from song
join album using (album_id)
join artist using (artist_id)
join time using (song_played_at)

- create table final as 
select distinct 
song_played_at ,song_id,song_name, song_duration, song_popularity  ,   
album_id ,album_name,album_type,album_total_tracks  ,album_release_date  ,
artist_id , artist_name, artist_type  , song_played_date  ,  
month , day, week_number,weekday   
from song
join album using (album_id)
join artist using (artist_id)
join time using (song_played_at)
## Step 3: Data Visualization with Quicksight

