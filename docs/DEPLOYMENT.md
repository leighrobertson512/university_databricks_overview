# Deployment Guide

This project uses Databricks Asset Bundles for deployment and management.

## Prerequisites

- Databricks CLI installed and configured
- Access to the target workspaces (field_eng or fevm_ws)
- Proper authentication configured in `.databrickscfg`

## Configuration

The `databricks.yml` file defines:
- **Bundle name**: `university_databricks_overview`
- **Targets**: Two workspace targets (field_eng and fevm_ws)
- **Variables**: Workspace-specific configurations (cluster IDs, paths)

## Deployment Commands

### Validate Configuration

```bash
databricks bundle validate
```

### Deploy to Default Target (field_eng)

```bash
databricks bundle deploy
```

### Deploy to Specific Target

```bash
# Deploy to field_eng workspace
databricks bundle deploy -t field_eng

# Deploy to fevm_ws workspace
databricks bundle deploy -t fevm_ws
```

### Run Jobs

```bash
# Run a job on the default target
databricks bundle run <job_name>

# Run a job on a specific target
databricks bundle run <job_name> -t fevm_ws
```

### Check Deployment Status

```bash
databricks bundle validate -t field_eng
```

## Adding Jobs

To add a new job to the bundle, edit `databricks.yml` and add it under `resources.jobs`:

```yaml
resources:
  jobs:
    your_job_name:
      name: "Your Job Display Name"
      email_notifications:
        on_failure:
          - leigh.robertson@databricks.com
      tasks:
        - task_key: "your_task"
          existing_cluster_id: ${var.primary_cluster_id}
          spark_python_task:
            python_file: "src/pipelines/your_pipeline.py"
            parameters:
              - "--param1"
              - "value1"
```

## Workspace Targets

### field_eng (Default)
- Host: `https://e2-demo-field-eng.cloud.databricks.com`
- Profile: `field_eng`
- Cluster ID: `0709-132523-cnhxf2p6`

### fevm_ws
- Host: `https://fevm-leigh-robertson-demo-ws.cloud.databricks.com`
- Profile: `fevm_ws`
- Cluster ID: `0113-012705-3e9ykmog`

## Troubleshooting

### Authentication Issues
Ensure your `.databrickscfg` file has the correct profiles configured:
```bash
databricks configure --profile field_eng
databricks configure --profile fevm_ws
```

### Validation Errors
Run validation to check for configuration issues:
```bash
databricks bundle validate
```

### Cluster Not Found
Verify the cluster IDs in `databricks.yml` match existing clusters in your workspace.

## Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)


