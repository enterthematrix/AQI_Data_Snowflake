-- This is the final script that creates a wide table with the AQI index and the prominent pollutant for each record in the CLEAN_FLATTEN_AQI_DT table.
-- The AQI index is calculated using the aqi_index function, and the prominent pollutant is determined using the prominent_pollutant function.


create or replace dynamic table aqi_final_wide_dt
    target_lag='30 min'
    warehouse=ADHOC_WH
as
select
        index_record_ts,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) aqi_day,
        hour(index_record_ts) aqi_hour,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_pollutant(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
        case
        when aqi_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
    from clean_flatten_aqi_dt;

-- Review the data:
show dynamic tables;
select * from AQI_FINAL_WIDE_DT limit 2;


