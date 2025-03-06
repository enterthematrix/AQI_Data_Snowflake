-- aggregated hourly AQI data for cities
create or replace dynamic table hour_level_agg_city_fact
    target_lag='30 min'
    warehouse=streamsetsses_wh
as
with city_level_data as (
select
    d.measurement_time,
    l.country as country,
    l.state as state,
    l.city as city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from
    air_quality_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
group by
    1,2,3,4
)
select
    *,
    prominent_pollutant(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when aqi_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from
    city_level_data;

select * from HOUR_LEVEL_AGG_CITY_FACT limit 2;

-- aggregated daily AQI data for cities
create or replace dynamic table daily_agg_city_fact
    target_lag='30 min'
    warehouse=streamsetsses_wh
as
with daily_data as (
select
    date(measurement_time) as measurement_date,
    country as country,
    state as state,
    city as city,
    round(avg(pm10_avg)) as pm10_avg,
    round(avg(pm25_avg)) as pm25_avg,
    round(avg(so2_avg)) as so2_avg,
    round(avg(no2_avg)) as no2_avg,
    round(avg(nh3_avg)) as nh3_avg,
    round(avg(co_avg)) as co_avg,
    round(avg(o3_avg)) as o3_avg
from
    HOUR_LEVEL_AGG_CITY_FACT
group by
    1,2,3,4
)
select
    *,
    prominent_pollutant(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when aqi_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from
    daily_data;

select * from DAILY_AGG_CITY_FACT limit 2;