# Utilities

This directory contains shared utility functions and helpers.

## Utility Functions

Common utilities include:
- Spark session management
- Logging helpers
- Configuration loaders
- Data validation functions

## Example Utility

```python
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """Create and return a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()
```

