# NOAA Weather Ingestion (Databricks ETL Sample)

This project is a **beginner-friendly Databricks ETL sample** that ingests weather data from **publicly available sources** into Delta tables. It’s designed for people who want a realistic dataset to practice common patterns like **parameterized ingestion**, **bronze/silver table layout**, and **idempotent upserts**.

## What you’ll build

- **Bronze**: `zip_code` reference table (ZIP → lat/long + state metadata)
- **Bronze**: `forecasts` table (NOAA forecast periods per ZIP code)
- **Optional Silver**: `forecasts_expanded` table scaffold for flattening/curating forecasts

## Public data sources used

- **Zippopotam.us (ZIP reference data)**: `https://www.zippopotam.us/`
- **NOAA / National Weather Service forecasts (via `noaa_sdk`)**
  - The notebook installs `noaa_sdk` via `%pip install noaa_sdk`

## Project files (run in this order)

All notebooks/scripts live directly in this folder:

1. `00_variables.py`: central configuration (catalog/schema, ZIP range, default state)
2. `01_set_up_work.py`: creates schemas/tables (DDL + optional constraints)
3. `utils_file.py`: shared helper functions (MERGE SQL helpers + small transforms)
4. `02_zip_code.py`: loads ZIP code reference data from Zippopotam.us
5. `03_weather_ingest.py`: loads NOAA forecasts for ZIP codes in a given state

## Prerequisites

- **Databricks workspace** with permissions to create schemas/tables in your target catalog
- **A cluster** that can run Python + Spark
- **Python packages** used in the notebooks:
  - `requests`
  - `pandas`
  - `noaa_sdk` (installed in `03_weather_ingest.py`)

## Setup and run instructions (traditional ETL)

### Step 1: Configure your environment

Open `00_variables.py` and update:

- **Catalog/schemas**: `catalog`, `bronze_schema`, `silver_schema`
- **ZIP backfill range**: `start_zip`, `end_zip`
  - Recommendation: start small while learning (example: `"80200"` → `"80220"`)
- **Default ingest state**: `default_state` (example: `"CO"`)

### Step 2: Create schemas and tables

Run `01_set_up_work.py`.

- The DDL execution is controlled by commented `spark.sql(...)` lines in the notebook.
- If you want tables created automatically, **uncomment** the relevant `spark.sql(...)` calls before running.

### Step 3: Backfill ZIP reference data (bronze)

Run `02_zip_code.py`.

- This iterates over ZIP codes in the configured range and calls Zippopotam.us.
- A `time.sleep(5)` is included to be polite to the public API; large ranges can take a long time.
- The load uses MERGE/upsert logic, so it’s intended to be **safe to re-run**.

### Step 4: Ingest NOAA forecasts (bronze)

Run `03_weather_ingest.py`.

- A widget named `state` controls which state’s ZIP codes are ingested.
- The notebook reads ZIP codes for that state from the `zip_code` table, then loads forecasts per ZIP.
- It finishes by running `OPTIMIZE` and `VACUUM` on the forecasts table (you can remove these if you want a faster/cheaper demo run).

## Notes and troubleshooting

- **Start small**: keep `start_zip/end_zip` narrow until you’re confident everything is working.
- **Public API variability**: requests can fail intermittently (timeouts, throttling, transient errors). Re-running is expected.
- **Helper utilities**: `02_zip_code.py` and `03_weather_ingest.py` use `%run ./utils_file` for shared helper functions (dynamic MERGE SQL generation).

