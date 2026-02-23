-- Seed baseline dataset configuration if the table is empty.
-- This avoids a chicken-and-egg problem during the first ETL run.

INSERT INTO dataset_config (
    dataset_id,
    source_name,
    source_url,
    format,
    update_frequency,
    is_active
) VALUES
('mrds',
 'USGS Mineral Resources Data System',
 'https://mrdata.usgs.gov/mrds/',
 'zip',
 'irregular',
 TRUE),
('worldbank_gdp',
 'World Bank GDP (NY.GDP.MKTP.CD)',
 'https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD',
 'json',
 'annual',
 TRUE),
('worldbank_population',
 'World Bank Population (SP.POP.TOTL)',
 'https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL',
 'json',
 'annual',
 TRUE),
('fsi_2023',
 'Fragile States Index 2023',
 'https://fragilestatesindex.org/',
 'xlsx',
 'annual',
 TRUE),
('cpi_2023',
 'Transparency International CPI 2023',
 'https://www.transparency.org/en/cpi/2023',
 'xlsx',
 'annual',
 TRUE)
ON CONFLICT (dataset_id) DO NOTHING;
