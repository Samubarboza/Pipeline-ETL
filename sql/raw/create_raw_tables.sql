CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    payload JSONB,
    source_system TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS raw_fx (
    id SERIAL PRIMARY KEY,
    payload JSONB,
    source_system TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS raw_crypto (
    id SERIAL PRIMARY KEY,
    payload JSONB,
    source_system TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    execution_date DATE
);
