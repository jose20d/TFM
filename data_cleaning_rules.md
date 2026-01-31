# Data Cleaning Cases and Rules (Point 3)

This document lists the **explicit cleaning cases per file** and the rules to apply before using the data for filtering and indicators. It is intended to be used as the specification for the cleaning script.

General rules (apply to all datasets):
- Trim whitespace in text fields and normalize casing where appropriate.
- Any **null/empty** numeric indicator value becomes `NA` (or `null` in JSON).
- If a numeric field contains non-numeric text (letters where only numbers are expected), mark the row as invalid and exclude it from indicator outputs.
- All removals should be logged in a cleaning report with the reason.

---

## worldbank_population.xlsx

**Cases / rules**
- Remove non-country aggregate rows (World Bank regions and income groups).
  - Examples (non-exhaustive): `Africa Eastern and Southern`, `Africa Western and Central`, `Arab World`, `East Asia & Pacific`, `Europe & Central Asia`, `Latin America & Caribbean`, `Middle East & North Africa`, `North America`, `South Asia`, `Sub-Saharan Africa`, `World`, `High income`, `Upper middle income`, `Lower middle income`, `Low income`, `OECD members`, `Euro area`.
- For each country, **use the latest available year** with a non-null value.
- Output label: **`Poblacion (YEAR)`**.
- Values must be numeric and > 0. Otherwise, set to `NA` and exclude from metrics.

---

## worldbank_gdp.xlsx

**Cases / rules**
- Remove non-country aggregate rows (same rule as population).
- For each country, **use the latest available year** with a non-null value.
- Output label: **`PIB (YEAR)`**.
- Values must be numeric and > 0. Otherwise, set to `NA` and exclude from metrics.

---

## CPI2023_Global_Results__Trends.xlsx

**Cases / rules**
- Keep only valid countries in `Country / Territory`; remove regions or aggregates.
- Use **`CPI score 2023`** for each country.
- Values must be numeric between **0 and 100**.
- Missing values are set to `NA`.

---

## FSI-2023-DOWNLOAD.xlsx

**Cases / rules**
- Keep only valid countries; remove regions or aggregates.
- Use **`Rank`** as the indicator labeled **`Indice de fragilidad (Rank 2023)`**.
- `Rank` must be numeric (integer). Missing or non-numeric becomes `NA`.

---

## MRDS Location.csv

**Cases / rules**
- `country` must be a valid country name (use aliases + ISO list).
- Remove rows where `country` is a **region** or a **region code** (e.g., `AF`, `EU`, `AS`, `OC`, `SA`, `CR`).
- Remove rows with empty `country`.
- Normalize `state_prov` by removing trailing special characters (e.g., `Roraima*` → `Roraima`).

---

## MRDS.csv

**Cases / rules**
- `latitude` must be numeric in range **[-90, 90]**.
- `longitude` must be numeric in range **[-180, 180]**.
- Rows with invalid coordinates are excluded from geo-dependent outputs.

---

## Ownership.csv

**Cases / rules**
- `pct` must be numeric between **0 and 100**. Otherwise set to `NA`.
- `beg_yr`, `end_yr`, `info_yr` must be 4-digit years in a reasonable range (e.g., 1800–current year).
- If both `beg_yr` and `end_yr` are present, **`beg_yr <= end_yr`**.

---

## Ages.csv

**Cases / rules**
- `age_young`, `age_old`, `age_young_ba`, `age_old_ba` must be numeric if present.
- If both young and old ages are present, enforce **`age_young <= age_old`**.
- Negative ages are invalid.

---

## Commodity.csv

**Cases / rules**
- Validate that `code` and key categorical fields match expected formats (upper-case or standard tokens).
- Flag rows where numeric fields contain letters.

---

## Materials.csv

**Cases / rules**
- `ore_gangue` must be either **`Ore`** or **`Gangue`** (case-insensitive).
- Flag rows where numeric fields contain letters.

---

## Rocks.csv / Physiography.csv / Other MRDS tables

**Cases / rules**
- `dep_id` must exist and be non-empty.
- Flag rows where numeric fields contain letters.
- Remove obvious region names from any country-related fields if present.

