# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# MAGIC SELECT count(*)
# MAGIC FROM serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded

# COMMAND ----------


