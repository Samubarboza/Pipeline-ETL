INSERT INTO staging_weather (
    weather_timestamp,
    city,
    country,
    temp_celsius,
    humidity,
    wind_speed,
    execution_date
)
SELECT
    to_timestamp((payload->>'dt')::bigint),
    COALESCE(payload->>'name', source_system) AS city,
    payload->'sys'->>'country' AS country,
    (payload->'main'->>'temp')::float AS temp_celsius,
    (payload->'main'->>'humidity')::float AS humidity,
    (payload->'wind'->>'speed')::float AS wind_speed,
    execution_date
FROM raw_weather
WHERE execution_date = '{{ ds }}';

