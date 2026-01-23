# Transformations

This directory contains data transformation logic and business rules.

## Transformation Guidelines

- Keep transformations focused on a single concern
- Use clear, descriptive function names
- Document complex business logic
- Write unit tests for transformation functions

## Example Transformation

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_data(df: DataFrame) -> DataFrame:
    """Example transformation function."""
    return df.withColumn(
        "cleaned_column",
        when(col("column").isNull(), "default").otherwise(col("column"))
    )
```

