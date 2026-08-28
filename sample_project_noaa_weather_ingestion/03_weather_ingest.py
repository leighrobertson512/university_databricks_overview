# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "noaa_sdk",
# ]
# ///
#Historical weather data: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

# COMMAND ----------

# MAGIC %pip install noaa_sdk

# COMMAND ----------

# MAGIC %run ./00_variables

# COMMAND ----------

from noaa_sdk import NOAA
import pandas as pd
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# MAGIC %run ./utils_file

# COMMAND ----------

dbutils.widgets.text('state', default_state)

# COMMAND ----------

def get_and_load_forecasts(postal_code, country_code):
    n = NOAA()
    forecasts = n.get_forecasts(postal_code, country_code)

    df = pd.DataFrame(forecasts)
    df_spark = spark.createDataFrame(df).withColumn('post_code', lit(postal_code)).withColumn('audit_update_ts', lit(current_timestamp()))
    df_spark.createOrReplaceTempView('forecasts')

    table_name = forecast_table_name
    match_columns, update_columns, insert_columns = generate_match_insert_columns("post_code, startTime", df_spark)
    merge_sql = dynamic_merge_sql(table_name, "updates", match_columns, update_columns, insert_columns)
    df_spark.createOrReplaceTempView("updates")
    spark.sql(merge_sql)
    print(f'loaded forecasts for {postal_code} and country {country_code}')

# COMMAND ----------

state = dbutils.widgets.get('state')
state_forecasts = f"""
SELECT distinct post_code
FROM {zip_code_table_name}
WHERE state_abbreviation = '{state}'
"""
df = spark.sql(state_forecasts)
for row in df.collect():
    postal_code = row['post_code']
    try:
        get_and_load_forecasts(postal_code, default_country_code)
    except Exception as e:
        print(f'Failed to load forecasts for {postal_code}: {e}')

# COMMAND ----------


optimize_sql = f"OPTIMIZE {forecast_table_name};"
vacuum_sql = f"VACUUM {forecast_table_name};"
spark.sql(optimize_sql)
spark.sql(vacuum_sql)

# COMMAND ----------

# DBTITLE 1,Load forecasts for hardcoded zip codes
zip_codes = ['85003', '48201', '28202', '43216', '97205', '15222', '75099', '53203']

for postal_code in zip_codes:
    try:
        get_and_load_forecasts(postal_code, default_country_code)
    except Exception as e:
        print(f'Failed to load forecasts for {postal_code}: {e}')

# COMMAND ----------

# DBTITLE 1,Total record count - bronze forecasts
zip_codes = "'85003','48201','28202','43216','97205','15222','75099','53203'"
count_df = spark.sql(f"SELECT COUNT(*) AS total_records FROM {forecast_table_name} WHERE post_code IN ({zip_codes})")
display(count_df)

# COMMAND ----------


