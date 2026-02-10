# University Alliance World Cup Demo – Setup Instructions

This guide walks you through setting up the World Cup data demo for the **Databricks University Alliance Program**. Follow the steps in order.

---

## Prerequisites

- A free Databricks account (see Step 1)
- Access to the demo repository (see Step 2)

---

## Step 1: Create a Free Databricks Account

1. Go to the Databricks free edition sign-up page:
   - **https://docs.databricks.com/aws/en/getting-started/free-edition**
2. Create a free account and complete the setup.
3. You will receive a Databricks workspace URL (e.g. `https://xxxxxxxx.cloud.databricks.com`).

---

## Step 2: Clone the Repo Locally and in Databricks

### Local clone (for file access)
- Clone the repository locally:
  ```bash
  git clone https://github.com/leighrobertson512/university_databricks_overview
  ```
- This gives you access to all notebooks, SQL files, and CSV datasets on your machine.

### Git folder in Databricks 
- In your Databricks workspace, go to **Repos** and create a new repo.
- Connect it to: `https://github.com/leighrobertson512/university_databricks_overview`
- Use this to sync notebooks and run directly from Databricks.

**Teaching tip:** Use this step to introduce the Databricks UI (workspace, Repos, clusters, etc.).

---

## Step 3: Run Setup – Create Catalog, Schema, Volume, and Tables

**File:** `01_set_up.ipynb`

1. Open `01_set_up.ipynb` in Databricks (or upload it from your local clone).
2. Run the cells in order:
   - **Cell 1:** Creates the catalog `university_learning`, schema `world_cup`, and volume `world_cup_data`.
   - **Cell 2:** Runs the DDL statements to create all 27 Delta tables (empty at this stage).
3. Ensure all cells complete successfully before proceeding.

**Why clone locally?** Cloning locally ensures you have all files (notebooks, SQL, CSVs) in one place so you can easily upload or copy them into Databricks.

---

## Step 4: Load Data into Databricks

**File:** `02_load_data.ipynb`

1. **Upload CSV files to the volume:**
   - In Databricks, go to **Catalog** → **university_learning** → **world_cup** → **world_cup_data** (volume).
   - Upload all CSV files from the `data/` folder in the repo into this volume.
   - The CSV files should be at: `university_learning.world_cup.world_cup_data`

2. **Run the load notebook:**
   - Open `02_load_data.ipynb` and run the notebook.
   - It reads all CSVs from the volume, adds `audit_update_ts`, and writes them into the Delta tables.
   - Each table is truncated before load to avoid duplicates in this demo.

---

## Step 5: Add Primary and Foreign Key Relationships

**File:** `03_relationships.ipynb`

1. Open `03_relationships.ipynb`.
2. Run all cells in order:
   - **Cell 1:** Creates the schema `university_learning.world_cup_agents` (for future use).
   - **Remaining cells:** Add primary key and foreign key constraints to the `world_cup` tables.
3. These constraints are **informational** in Unity Catalog (not enforced) but help query optimization and BI tools.

---

## Optional: Build Dashboards

**File:** `dashboard_queries.sql`

- Use the queries in `dashboard_queries.sql` to build dashboards or reports.
- The file includes sections for:
  - Tournament overview and KPIs
  - Top scorers
  - Team performance
  - And more

---

## File Reference

| File | Purpose |
|------|---------|
| `01_set_up.ipynb` | Create catalog, schema, volume, and empty tables (DDL) |
| `02_load_data.ipynb` | Load CSV data from the volume into Delta tables |
| `03_relationships.ipynb` | Add primary and foreign key constraints |
| `dashboard_queries.sql` | Sample SQL for dashboards and analytics |
| `instructions_read_me.md` | This setup guide |

---

## Troubleshooting

- **Volume not found:** Ensure Step 3 completed and the volume `world_cup_data` exists under `university_learning.world_cup`.
- **Tables not found:** Run `01_set_up.ipynb` before `02_load_data.ipynb`.
- **Duplicate data:** `02_load_data.ipynb` truncates tables before load; re-running it will replace existing data.
