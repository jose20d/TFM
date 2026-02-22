-- Indexes for the TFM project schema.
-- This script is idempotent and safe to run multiple times.

-- Spatial index for fast geographic queries.
CREATE INDEX IF NOT EXISTS idx_mrds_deposit_geom
    ON mrds_deposit
    USING GIST (geom);

-- Lookups by country.
CREATE INDEX IF NOT EXISTS idx_mrds_location_country
    ON mrds_location (country_id);

-- Common joins by deposit.
CREATE INDEX IF NOT EXISTS idx_mrds_commodity_dep
    ON mrds_commodity (dep_id);

CREATE INDEX IF NOT EXISTS idx_mrds_rocks_dep
    ON mrds_rocks (dep_id);

-- Country indicators lookups.
CREATE INDEX IF NOT EXISTS idx_country_indicator_country_dataset_year
    ON country_indicator (country_id, dataset_id, year);
