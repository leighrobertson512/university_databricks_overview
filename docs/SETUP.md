# Setup Guide

## Virtual Environment Setup with `uv`

This project uses `uv` as the package manager for faster dependency installation.

### Using `uv` with the Virtual Environment

You have two options for using `uv`:

#### Option 1: Use `uv pip` (Recommended for existing venv)

With your existing virtual environment, you can use `uv pip` as a drop-in replacement for `pip`:

```bash
# Activate your virtual environment
source university-databricks-overview/bin/activate

# Install dependencies using uv
uv pip install -r requirements.txt

# Install a new package
uv pip install package-name

# Freeze requirements
uv pip freeze > requirements.txt
```

#### Option 2: Use `uv` to manage the virtual environment directly

Alternatively, you can use `uv` to create and manage virtual environments:

```bash
# Create a virtual environment with uv
uv venv university-databricks-overview

# Activate it
source university-databricks-overview/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

### Benefits of `uv`

- **Much faster** than `pip` (10-100x faster)
- **Drop-in replacement** for `pip` commands
- **Better dependency resolution**
- **Compatible** with existing `requirements.txt` files

### Common Commands

```bash
# Install from requirements.txt
uv pip install -r requirements.txt

# Install a single package
uv pip install package-name

# Install with version constraint
uv pip install "package-name>=1.0.0"

# Uninstall a package
uv pip uninstall package-name

# List installed packages
uv pip list

# Show package information
uv pip show package-name

# Freeze current environment
uv pip freeze > requirements.txt

# Sync environment to match requirements.txt exactly
uv pip sync requirements.txt
```

### Updating Dependencies

```bash
# Update all packages
uv pip install --upgrade -r requirements.txt

# Update a specific package
uv pip install --upgrade package-name
```

