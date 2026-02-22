-- Database schema for the TFM project.
-- This script is idempotent and safe to run multiple times.

-- Enable PostGIS extension if it is not already available.
CREATE EXTENSION IF NOT EXISTS postgis;

-- =========================
-- Metadata tables
-- =========================

CREATE TABLE IF NOT EXISTS dataset_config (
    dataset_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    format TEXT NOT NULL,
    update_frequency TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_load_log (
    load_id SERIAL PRIMARY KEY,
    dataset_id TEXT REFERENCES dataset_config(dataset_id),
    raw_filename TEXT NOT NULL,
    file_hash TEXT,
    rows_inserted INTEGER,
    rows_failed INTEGER,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- Dimensions
-- =========================

CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL,
    country_norm TEXT UNIQUE NOT NULL,
    iso3 CHAR(3)
);

CREATE TABLE IF NOT EXISTS dim_dataset (
    dataset_id TEXT PRIMARY KEY,
    source_name TEXT,
    source_url TEXT,
    raw_filename TEXT,
    updated_at TIMESTAMP
);

-- =========================
-- MRDS core tables
-- =========================

CREATE TABLE IF NOT EXISTS mrds_deposit (
    dep_id BIGINT PRIMARY KEY,
    name TEXT,
    dev_stat TEXT,
    code_list TEXT,
    geom geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS mrds_location (
    dep_id BIGINT PRIMARY KEY REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    country_id INTEGER REFERENCES dim_country(country_id),
    state_prov TEXT,
    region TEXT,
    county TEXT
);

-- =========================
-- MRDS related tables (1-N)
-- =========================

CREATE TABLE IF NOT EXISTS mrds_commodity (
    commodity_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    commod TEXT,
    code TEXT,
    commod_tp TEXT,
    commod_group TEXT,
    import TEXT
);

CREATE TABLE IF NOT EXISTS mrds_material (
    material_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    rec TEXT,
    ore_gangue TEXT,
    material TEXT
);

CREATE TABLE IF NOT EXISTS mrds_ownership (
    ownership_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    owner_name TEXT,
    owner_tp TEXT
);

CREATE TABLE IF NOT EXISTS mrds_physiography (
    physiography_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    phys_div TEXT,
    phys_prov TEXT,
    phys_sect TEXT,
    phys_det TEXT
);

CREATE TABLE IF NOT EXISTS mrds_ages (
    age_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    age_tp TEXT,
    age_young TEXT
);

CREATE TABLE IF NOT EXISTS mrds_rocks (
    rock_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    rock_cls TEXT,
    first_ord_nm TEXT,
    second_ord_nm TEXT,
    third_ord_nm TEXT,
    low_name TEXT
);

-- =========================
-- Country indicators
-- =========================

CREATE TABLE IF NOT EXISTS country_indicator (
    indicator_id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES dim_country(country_id),
    dataset_id TEXT REFERENCES dim_dataset(dataset_id),
    year INTEGER NOT NULL,
    value NUMERIC,
    UNIQUE(country_id, dataset_id, year)
);
