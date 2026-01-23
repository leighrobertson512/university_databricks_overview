# Pipelines

This directory contains data pipeline definitions for ETL/ELT processes.

## Pipeline Structure

Each pipeline should:
- Follow PySpark best practices
- Use Delta Lake for all table operations
- Implement proper error handling and logging
- Be modular and reusable

## Example Pipeline

```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def ingest_pipeline(spark: SparkSession, source_path: str, target_table: str):
    """Example pipeline function."""
    df = spark.read.format("delta").load(source_path)
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
```

