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

## Step 3: Data Visualization with Quicksight

