-- load some duplicate data into the stage @ data/raw_data/set-B-duplicate-data
-- records with same 'index_record_ts' will be duplicate

-- creating dynamic table from de-duplicated raw data + flattened records
create or replace dynamic table clean_aqi_dt
    target_lag='downstream'
    warehouse=STREAMSETSSES_WH
as
WITH air_quality_with_rank AS (
    SELECT
        id,
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts,
        -- Rank the files in descending order of upload timestamp
        ROW_NUMBER() OVER (PARTITION BY index_record_ts ORDER BY _stg_file_load_ts DESC) AS latest_file_rank
    FROM
        raw_aqi
    WHERE
        index_record_ts IS NOT NULL
),
unique_air_quality_data AS (
    SELECT
        *
    FROM
        air_quality_with_rank
    WHERE
        latest_file_rank = 1
)
select
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;

-- check the sample data
    select * from CLEAN_AQI_DT
    where
        country = 'India' and
        state = 'Karnataka' and
        station = 'Silk Board, Bengaluru - KSPCB'
    limit 10;
