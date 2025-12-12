-- sacamos los datos de stg_weather y dim_city y lo vamos a guardar en fact_weather_readings
INSERT INTO fact_weather_readings (
    city_id,
    weather_timestamp,
    temp_celsius,
    humidity,
    wind_speed,
    execution_date
)
SELECT
    dc.city_id,
    sw.weather_timestamp,
    sw.temp_celsius,
    sw.humidity,
    sw.wind_speed,
    sw.execution_date
FROM staging_weather sw
JOIN dim_city dc
    ON sw.city = dc.city
    AND sw.country = dc.country
WHERE sw.execution_date = '{{ ds }}'
ON CONFLICT (city_id, weather_timestamp, execution_date) DO NOTHING;
