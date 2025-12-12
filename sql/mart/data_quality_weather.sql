-- primero temperaturas fuera de rango
SELECT COUNT(*) AS invalid_temp
FROM fact_weather_readings
WHERE temp_celsius < -50 OR temp_celsius > 60;

-- fechas nulas
SELECT COUNT(*) AS null_dates
FROM fact_weather_readings
WHERE weather_timestamp IS NULL;

-- city id nulo
SELECT COUNT(*) AS null_city
FROM fact_weather_readings
WHERE city_id IS NULL;
