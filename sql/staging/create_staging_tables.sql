CREATE TABLE IF NOT EXISTS stg_weather (
    weather_timestamp TIMESTAMP,
    city TEXT,
    country TEXT,
    temp_celsius FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS stg_fx (
    fx_timestamp TIMESTAMP,
    base_currency TEXT,
    target_currency TEXT,
    rate FLOAT,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS stg_crypto (
    crypto_timestamp TIMESTAMP,
    asset TEXT,
    price_usd FLOAT,
    execution_date DATE
);
