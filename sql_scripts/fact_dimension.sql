-- BUILD FACT & DIMENSION TABLES

-- DATE dimension

create or replace dynamic table date_dim
target_lag='DOWNSTREAM'
warehouse=streamsetsses_wh
as
with hour_granularity as (
select
        index_record_ts as measurement_time,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) aqi_day,
        hour(index_record_ts)+1 aqi_hour,
    from
        clean_flatten_aqi_dt
        group by 1,2,3,4,5,6
)
select
    hash(measurement_time) as date_pk,
    *
from hour_granularity
order by aqi_year,aqi_month,aqi_day,aqi_hour;

-- LOCATION dimension
create or replace dynamic table location_dim
    target_lag='DOWNSTREAM'
    warehouse=streamsetsses_wh
as
with location as (
select
    LATITUDE,
    LONGITUDE,
    COUNTRY,
    STATE,
    CITY,
    STATION,
from
    clean_flatten_aqi_dt
    group by 1,2,3,4,5,6
)
select
    hash(LATITUDE,LONGITUDE) as location_pk,
    *
from location
order by
    country, STATE, city, station;

-- AQI fact table
create or replace dynamic table air_quality_fact
    target_lag='30 min'
    warehouse=streamsetsses_wh
as
select
        hash(index_record_ts,latitude,longitude) aqi_pk,
        hash(index_record_ts) as date_fk,
        hash(latitude,longitude) as location_fk,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_pollutant(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when aqi_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG) > 2 then greatest (PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)
        else 0
        end
    as aqi
    from clean_flatten_aqi_dt

select * from air_quality_fact limit 10;