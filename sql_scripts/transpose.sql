-- transpose the data from rows to columns to a temporary table.

-- validate the query with a single record
create temp table air_quality_tmp as
select
        index_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        -- just take the avg reading for each pollutant id
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from
        clean_aqi_dt
    where
        country = 'India' and
        state = 'Karnataka' and
        station = 'Silk Board, Bengaluru - KSPCB' and
        index_record_ts = '2024-03-01 11:00:00.000'
     group by
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station;

-- check the data
select * from air_quality_tmp;

-- run the query for entire dataset
create or replace temp table air_quality_tmp as
select
        index_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from
        clean_aqi_dt
     group by
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station;

-- HANDLE MISSING DATA
-- lots of rows with missing data, for example below:
select * from air_quality_tmp
where
        country = 'India' and
        state = 'Andhra_Pradesh' and
        station = 'Gangineni Cheruvu, Chittoor - APPCB' and
        index_record_ts = '2024-03-01 23:00:00.000';

-- Fix the missing data with default values
-- test query for a single record
select
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE
            WHEN PM10_AVG = 'NA' THEN 0
            WHEN PM10_AVG is Null THEN 0
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE
            WHEN PM25_AVG = 'NA' THEN 0
            WHEN PM25_AVG is Null THEN 0
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE
            WHEN SO2_AVG = 'NA' THEN 0
            WHEN SO2_AVG is Null THEN 0
            ELSE round(SO2_AVG)
        END as SO2_AVG,
         CASE
            WHEN NH3_AVG = 'NA' THEN 0
            WHEN NH3_AVG is Null THEN 0
            ELSE round(NH3_AVG)
        END as NH3_AVG,
        CASE
            WHEN NO2_AVG = 'NA' THEN 0
            WHEN NO2_AVG is Null THEN 0
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE
            WHEN CO_AVG = 'NA' THEN 0
            WHEN CO_AVG is Null THEN 0
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE
            WHEN O3_AVG = 'NA' THEN 0
            WHEN O3_AVG is Null THEN 0
            ELSE round(O3_AVG)
        END as O3_AVG,
    from air_quality_tmp
    where
        country = 'India' and
        state = 'Andhra_Pradesh' and
        station = 'Gangineni Cheruvu, Chittoor - APPCB' and
        index_record_ts = '2024-03-01 23:00:00.000';


-- create a flattened raw data table + replace NULL or 'NA' measurements with 0
-- drop table clean_flatten_aqi_dt;
create or replace dynamic table clean_flatten_aqi_dt
    target_lag='30 min'
    warehouse=ADHOC_WH
as
with flattened_pollutant_measurements as (
    SELECT
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM
        clean_aqi_dt
    group by
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station
),
replace_missing_measurements_with_default as (
    select
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE
            WHEN PM25_AVG = 'NA' THEN 0
            WHEN PM25_AVG is Null THEN 0
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE
            WHEN PM10_AVG = 'NA' THEN 0
            WHEN PM10_AVG is Null THEN 0
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE
            WHEN SO2_AVG = 'NA' THEN 0
            WHEN SO2_AVG is Null THEN 0
            ELSE round(SO2_AVG)
        END as SO2_AVG,
        CASE
            WHEN NO2_AVG = 'NA' THEN 0
            WHEN NO2_AVG is Null THEN 0
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE
            WHEN NH3_AVG = 'NA' THEN 0
            WHEN NH3_AVG is Null THEN 0
            ELSE round(NH3_AVG)
        END as NH3_AVG,
         CASE
            WHEN CO_AVG = 'NA' THEN 0
            WHEN CO_AVG is Null THEN 0
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE
            WHEN O3_AVG = 'NA' THEN 0
            WHEN O3_AVG is Null THEN 0
            ELSE round(O3_AVG)
        END as O3_AVG,
    from flattened_pollutant_measurements
)
select *
from replace_missing_measurements_with_default;

-- validate the data with the test record
select * from clean_flatten_aqi_dt
where
        country = 'India' and
        state = 'Andhra_Pradesh' and
        station = 'Gangineni Cheruvu, Chittoor - APPCB' and
        index_record_ts = '2024-03-01 23:00:00.000';

