-- Create a database and schema for the air quality data
CREATE DATABASE IF NOT EXISTS streamsetsses_db;
USE DATABASE streamsetsses_db;
CREATE SCHEMA IF NOT EXISTS aqi_schema;
USE SCHEMA aqi_schema;

-- create an internal stage and enable directory service
CREATE STAGE IF NOT EXISTS aqi_raw_data_stg
directory = ( enable = true)
comment = 'Internal Stage to store raw air quality data';


-- create file format to process the JSON file
CREATE FILE FORMAT IF NOT EXISTS json_file_format
type = 'JSON' compression = 'AUTO'
comment = 'JSON file format object';


SHOW STAGES;
SHOW FILE FORMATS;

-- upload files under data/raw_data/set-A-day-1-data directory to the stage via Snowflake UI
LIST @aqi_raw_data_stg;

-- query the stage directly
SELECT t.$1
FROM   @aqi_schema.aqi_raw_data_stg (file_format => json_file_format) t;

-- query metadata
SELECT try_to_timestamp(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
       t.$1,
       t.$1:total::int    AS record_count,
       t.$1:version::text AS json_version
FROM   @aqi_schema.aqi_raw_data_stg
    (file_format => JSON_FILE_FORMAT) t;

-- query stage file metadata

SELECT try_to_timestamp(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
       t.$1,
       t.$1:total::int    AS record_count,
       t.$1:version::text AS json_version,
       -- meta data information
       metadata$filename           AS _stg_file_name,
       metadata$file_last_modified AS _stg_file_load_ts,
       metadata$file_content_key   AS _stg_file_md5,
       CURRENT_TIMESTAMP()         AS _copy_data_ts
FROM   @aqi_schema.aqi_raw_data_stg (file_format => json_file_format) t;
