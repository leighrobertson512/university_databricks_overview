# Tests

This directory contains test files for the project.

## Testing Guidelines

- Test locally with Databricks Connect before deploying
- Use sample datasets for development (e.g., `samples.nyctaxi.trips`)
- Implement unit tests for transformation logic
- Use pytest for test execution

## Running Tests

```bash
# Activate virtual environment
source university-databricks-overview/bin/activate

# Install test dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run tests with coverage
pytest tests/ --cov=src --cov-report=html
```

