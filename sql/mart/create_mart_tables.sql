CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_asset (
    asset_id SERIAL PRIMARY KEY,
    asset TEXT
);


CREATE TABLE IF NOT EXISTS fact_weather_readings (
    reading_id SERIAL PRIMARY KEY,
    city_id INT REFERENCES dim_city(city_id),
    weather_timestamp TIMESTAMP,
    temp_celsius FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS fact_fx_rates (
    fx_id SERIAL PRIMARY KEY,
    base_currency_id INT REFERENCES dim_currency(currency_id),
    target_currency_id INT REFERENCES dim_currency(currency_id),
    fx_timestamp TIMESTAMP,
    rate FLOAT,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS fact_asset_prices (
    price_id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES dim_asset(asset_id),
    crypto_timestamp TIMESTAMP,
    price_usd FLOAT,
    execution_date DATE
);
