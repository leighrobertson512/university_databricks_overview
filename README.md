# university_databricks_overview
Hosts all content needed for data events with Universities 

## Quick Start

### Setup Virtual Environment

This project uses `uv` for fast package management. To set up your environment:

```bash
# Activate the virtual environment
source university-databricks-overview/bin/activate

# Install dependencies using uv (much faster than pip!)
uv pip install -r requirements.txt
```

### Using `uv` Instead of `pip`

Simply replace `pip` commands with `uv pip`:

```bash
# Instead of: pip install package-name
uv pip install package-name

# Instead of: pip install -r requirements.txt
uv pip install -r requirements.txt

# Instead of: pip freeze > requirements.txt
uv pip freeze > requirements.txt
```

See [docs/SETUP.md](docs/SETUP.md) for more details.
