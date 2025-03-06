-- raw table to have air quality data
CREATE or REPLACE transient TABLE raw_aqi (
    id int primary key autoincrement,
    index_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version text not null,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

-- load raw table from stage
COPY INTO raw_aqi (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) FROM
(
    SELECT
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version,
        metadata$filename as _stg_file_name,
        metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY as _stg_file_md5,
        current_timestamp() as _copy_data_ts

   FROM @aqi_schema.aqi_raw_data_stg as t
)
file_format = (format_name = 'aqi_schema.JSON_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;

-- verify data load
select * from raw_aqi limit 10;

-- [OPTIONAL]task to load raw aqi table every hour
create or replace task copy_air_quality_data
    warehouse = ADHOC_WH
    schedule = 'USING CRON 0 * * * * Pacific/Auckland'
    comment = 'Runs every hour at the start of the hour in Auckland time'
as
copy into raw_aqi (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) from
(
    select
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version,
        metadata$filename as _stg_file_name,
        metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY as _stg_file_md5,
        current_timestamp() as _copy_data_ts

   from @aqi_schema.aqi_raw_data_stg as t
)
file_format = (format_name = 'aqi_schema.JSON_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;

use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

-- run the task
alter task copy_air_quality_data resume;


