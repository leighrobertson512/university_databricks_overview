# University Databricks Overview

This repository contains **Databricks University Alliance** teaching assets: demo notebooks, sample datasets, and a beginner-friendly ETL sample project you can run in a Databricks workspace.

## Start here

- **University Alliance demo notebooks**: `src/uva_university_alliance/`
  - Instructions: `src/uva_university_alliance/instructions_read_me.md`
- **Sample datasets (World Cup CSVs)**: `data/`
  - Notes: `data/README.md`
- **Public dataset ETL sample (NOAA weather ingestion)**: `sample_project_noaa_weather_ingestion/`
  - Project readme: `sample_project_noaa_weather_ingestion/noaa_weather_ingestion_read_me.md`

## Repository layout

- **`src/`**: notebooks, demos, and teaching materials
  - `src/uva_university_alliance/`
    - `01_set_up.ipynb`, `02_load_data.ipynb`, `03_relationships.ipynb`
    - `dashboard_queries.sql`
    - `04_demo_aibi_dashboard.lvdash.json` (Lakeview dashboard asset)
    - `University_Presentation .pdf` (presentation deck)
- **`data/`**: CSV datasets used by the notebooks (World Cup-related tables)
- **`sample_project_noaa_weather_ingestion/`**: Databricks ETL sample that ingests public NOAA/NWS forecast data into Delta tables
  - Notebooks/scripts: `00_variables.py`, `01_set_up_work.py`, `utils_file.py`, `02_zip_code.py`, `03_weather_ingest.py`
- **`.databricks/`**: Databricks Asset Bundle / deployment scaffolding
- **`.cursor/` / `.vscode/`**: editor configuration for working on this repo
- **`university-databricks-overview/`**: local Python environment directory (if you don’t intend to version this, consider removing it and adding it to `.gitignore`)
- **`LICENSE`**: repository license
