-- Database schema for the TFM project.
-- This script is idempotent and safe to run multiple times.

-- PostGIS is expected to be installed and enabled by the database administrator.

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
    dataset_id TEXT REFERENCES dataset_config(dataset_id) ON DELETE CASCADE,
    raw_filename TEXT NOT NULL,
    file_hash TEXT,
    file_size_bytes BIGINT,
    rows_inserted INTEGER,
    rows_failed INTEGER,
    load_status TEXT,
    error_message TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ETL state and run logs
-- =========================

CREATE TABLE IF NOT EXISTS etl_dataset_state (
    dataset_id TEXT PRIMARY KEY,
    last_hash TEXT,
    last_loaded_at TIMESTAMP,
    last_success BOOLEAN
);

CREATE TABLE IF NOT EXISTS etl_dataset_run_log (
    id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT,
    executed_at TIMESTAMP DEFAULT NOW(),
    download_success BOOLEAN,
    hash_value TEXT,
    has_changes BOOLEAN,
    load_success BOOLEAN,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    duration_ms INTEGER,
    error_message TEXT
);

-- Enforce allowed load statuses.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'etl_load_log_status_check'
    ) THEN
        ALTER TABLE etl_load_log
        ADD CONSTRAINT etl_load_log_status_check
        CHECK (load_status IN ('SUCCESS', 'FAILED'));
    END IF;
END $$;

-- =========================
-- Dimensions
-- =========================

CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL,
    country_norm TEXT UNIQUE NOT NULL,
    iso3 CHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ISO 3166-1 reference table (raw -> normalized).
CREATE TABLE IF NOT EXISTS iso_country_codes (
    iso_id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL,
    country_norm TEXT NOT NULL,
    iso2 CHAR(2),
    iso3 CHAR(3),
    iso_numeric TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure ISO3 uniqueness when present.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'iso_country_codes_iso3_unique'
    ) THEN
        ALTER TABLE iso_country_codes
        ADD CONSTRAINT iso_country_codes_iso3_unique
        UNIQUE (iso3);
    END IF;
END $$;

-- =========================
-- MRDS core tables
-- =========================

CREATE TABLE IF NOT EXISTS mrds_deposit (
    dep_id BIGINT PRIMARY KEY,
    name TEXT,
    dev_stat TEXT,
    code_list TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    geom geometry(Point, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_location (
    dep_id BIGINT PRIMARY KEY REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    country_id INTEGER REFERENCES dim_country(country_id) ON DELETE CASCADE,
    state_prov TEXT,
    region TEXT,
    county TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    import TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_material (
    material_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    rec TEXT,
    ore_gangue TEXT,
    material TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_ownership (
    ownership_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    owner_name TEXT,
    owner_tp TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_physiography (
    physiography_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    phys_div TEXT,
    phys_prov TEXT,
    phys_sect TEXT,
    phys_det TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_ages (
    age_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    age_tp TEXT,
    age_young TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mrds_rocks (
    rock_id SERIAL PRIMARY KEY,
    dep_id BIGINT REFERENCES mrds_deposit(dep_id) ON DELETE CASCADE,
    rock_cls TEXT,
    first_ord_nm TEXT,
    second_ord_nm TEXT,
    third_ord_nm TEXT,
    low_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- Country indicators
-- =========================

CREATE TABLE IF NOT EXISTS country_indicator (
    indicator_id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES dim_country(country_id) ON DELETE CASCADE,
    dataset_id TEXT REFERENCES dataset_config(dataset_id) ON DELETE CASCADE,
    indicator_code TEXT,
    year INTEGER NOT NULL,
    value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure composite uniqueness for indicators.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'country_indicator_unique_idx'
    ) THEN
        ALTER TABLE country_indicator
        ADD CONSTRAINT country_indicator_unique_idx
        UNIQUE (country_id, dataset_id, indicator_code, year);
    END IF;
END $$;

-- =========================
-- Idempotent column upgrades
-- =========================

ALTER TABLE etl_load_log
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS load_status TEXT,
    ADD COLUMN IF NOT EXISTS error_message TEXT;

ALTER TABLE mrds_deposit
    ADD COLUMN IF NOT EXISTS latitude NUMERIC(9,6),
    ADD COLUMN IF NOT EXISTS longitude NUMERIC(9,6),
    ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326),
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_location
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_commodity
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_material
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_ownership
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_physiography
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_ages
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE mrds_rocks
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE country_indicator
    ADD COLUMN IF NOT EXISTS indicator_code TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE dim_country
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE iso_country_codes
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
