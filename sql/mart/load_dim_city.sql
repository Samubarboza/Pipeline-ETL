-- insertamos datos en la tabla dim_city
INSERT INTO dim_city (city, country)
SELECT DISTINCT
    city,
    country
FROM staging_weather
WHERE city IS NOT NULL
    AND country IS NOT NULL
-- Si ya existe esa ciudad en dim_city, no la insertamos de nuevo
ON CONFLICT (city, country) DO NOTHING
