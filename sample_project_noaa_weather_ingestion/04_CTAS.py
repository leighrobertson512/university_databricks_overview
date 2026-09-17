# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE leigh_robertson_fevm_catalog.silver_noaa.forecasts_expanded_v2
# MAGIC CLUSTER BY (post_code, startTime, forecastDateLocal, forecastDateUTC)
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM leigh_robertson_fevm_catalog.silver_noaa.forecasts_expanded

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM leigh_robertson_fevm_catalog.silver_noaa.forecasts_expanded_v2
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM serverless_stable_7lg3y6_catalog.bronze_noaa.zip_code

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE serverless_stable_7lg3y6_catalog.silver_noaa.forecasts_expanded

# COMMAND ----------

