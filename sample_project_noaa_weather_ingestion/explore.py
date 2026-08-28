# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql 
# MAGIC SELECT state_abbreviation,
# MAGIC count(*)
# MAGIC FROM serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code
# MAGIC GROUP BY ALL 

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE serverless_stable_7lg3y6_catalog.bronze_noaa.forecasts SET TBLPROPERTIES
# MAGIC (delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --TRUNCATE TABLE  serverless_stable_7lg3y6_catalog.bronze_noaa.forecasts

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY serverless_stable_7lg3y6_catalog.bronze_noaa.forecasts;

# COMMAND ----------

# DBTITLE 1,Batch rebuild of forecasts_expanded (silver)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded AS
# MAGIC WITH transformed AS (
# MAGIC   SELECT
# MAGIC     post_code,
# MAGIC     number,
# MAGIC     name,
# MAGIC     regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime,
# MAGIC     regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime,
# MAGIC     regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
# MAGIC     isDaytime,
# MAGIC     temperature,
# MAGIC     temperatureUnit,
# MAGIC     temperatureTrend,
# MAGIC     CAST(regexp_extract(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed,
# MAGIC     windDirection,
# MAGIC     dewpoint.value AS dewpoint,
# MAGIC     probabilityOfPrecipitation.value AS probabilityOfPrecipitation,
# MAGIC     relativeHumidity.value AS relativeHumidity,
# MAGIC     icon,
# MAGIC     shortForecast,
# MAGIC     detailedForecast,
# MAGIC     current_timestamp() AS audit_update_ts
# MAGIC   FROM serverless_stable_7lg3y6_catalog.bronze_noaa.forecasts
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN timezoneOffset != '' THEN from_utc_timestamp(CAST(startTime AS TIMESTAMP), timezoneOffset)
# MAGIC     ELSE CAST(startTime AS TIMESTAMP)
# MAGIC   END AS startTimeUTC,
# MAGIC   CASE
# MAGIC     WHEN timezoneOffset != '' THEN from_utc_timestamp(CAST(endTime AS TIMESTAMP), timezoneOffset)
# MAGIC     ELSE CAST(endTime AS TIMESTAMP)
# MAGIC   END AS endTimeUTC
# MAGIC FROM transformed
# MAGIC WHERE windSpeed >= 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join each plant's lat/lng to the nearest zip code in bronze_noaa.zip_code
# MAGIC -- Uses Euclidean distance on lat/lng as a simple nearest-neighbor approximation
# MAGIC WITH plant_coords AS (
# MAGIC   SELECT DISTINCT plant_id, plant_lat, plant_lng
# MAGIC   FROM serverless_stable_7lg3y6_catalog.tech_summit_team_challenge.gold_line_status
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     p.plant_id,
# MAGIC     p.plant_lat,
# MAGIC     p.plant_lng,
# MAGIC     z.post_code,
# MAGIC     z.place_name,
# MAGIC     z.state,
# MAGIC     z.latitude AS zip_lat,
# MAGIC     z.longitude AS zip_lng,
# MAGIC     SQRT(POW(p.plant_lat - z.latitude, 2) + POW(p.plant_lng - z.longitude, 2)) AS distance,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY p.plant_id ORDER BY SQRT(POW(p.plant_lat - z.latitude, 2) + POW(p.plant_lng - z.longitude, 2))) AS rn
# MAGIC   FROM plant_coords p
# MAGIC   CROSS JOIN serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code z
# MAGIC )
# MAGIC SELECT
# MAGIC   plant_id,
# MAGIC   plant_lat,
# MAGIC   plant_lng,
# MAGIC   post_code,
# MAGIC   place_name,
# MAGIC   state,
# MAGIC   ROUND(distance, 4) AS distance_deg
# MAGIC FROM ranked
# MAGIC WHERE rn = 1
# MAGIC ORDER BY plant_id

# COMMAND ----------

# DBTITLE 1,Create plant-to-zip mapping table (working zips only)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE serverless_stable_7lg3y6_catalog.tech_summit_team_challenge.plant_zip_mapping AS
# MAGIC WITH plant_coords AS (
# MAGIC   SELECT DISTINCT plant_id, plant_lat, plant_lng
# MAGIC   FROM serverless_stable_7lg3y6_catalog.tech_summit_team_challenge.gold_line_status
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     p.plant_id,
# MAGIC     p.plant_lat,
# MAGIC     p.plant_lng,
# MAGIC     z.post_code,
# MAGIC     z.place_name,
# MAGIC     z.state,
# MAGIC     z.latitude AS zip_lat,
# MAGIC     z.longitude AS zip_lng,
# MAGIC     SQRT(POW(p.plant_lat - z.latitude, 2) + POW(p.plant_lng - z.longitude, 2)) AS distance,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY p.plant_id ORDER BY SQRT(POW(p.plant_lat - z.latitude, 2) + POW(p.plant_lng - z.longitude, 2))) AS rn
# MAGIC   FROM plant_coords p
# MAGIC   CROSS JOIN serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code z
# MAGIC   WHERE z.post_code IN ('85003', '48201', '28202', '43216', '97205', '15222', '75099', '53203')
# MAGIC )
# MAGIC SELECT
# MAGIC   plant_id,
# MAGIC   plant_lat,
# MAGIC   plant_lng,
# MAGIC   post_code,
# MAGIC   place_name,
# MAGIC   state,
# MAGIC   ROUND(distance, 4) AS distance_deg
# MAGIC FROM ranked
# MAGIC WHERE rn = 1
# MAGIC ORDER BY plant_id
