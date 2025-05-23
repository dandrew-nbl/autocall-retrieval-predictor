# ML Workspace

A containerized machine learning environment with MLflow, Jupyter Lab, and Factory Systems integration.

## Quick Start

1. **Start the services:**
```bash
docker-compose up -d
```

2. **Access your services:**
   - **Jupyter Lab**: http://localhost:8888 (no password required)
   - **MLflow UI**: http://localhost:5000

## Directory Structure

```
ml-workspace/
├── docker-compose.yml     # Docker services configuration
├── Dockerfile.jupyter     # Jupyter container definition
├── notebooks/            # Your Jupyter notebooks
├── data/                # Training data, CSVs, etc.
├── models/              # Saved models
├── mlflow-data/         # MLflow database and metadata
├── artifacts/           # MLflow artifacts (models, plots, etc.)
└── factory-data/        # Data shared with factory-systems container
```

## Usage

### Starting the Environment
```bash
docker-compose up -d
```

### Stopping the Environment
```bash
docker-compose down
```

### Viewing Logs
```bash
docker-compose logs -f jupyter
docker-compose logs -f mlflow
```

### Example ML Code
```python
import mlflow
import pandas as pd
import xgboost as xgb

# MLflow is automatically configured
mlflow.set_experiment("my-experiment")

with mlflow.start_run():
    # Your ML code here
    mlflow.log_param("learning_rate", 0.1)
    mlflow.log_metric("rmse", 6.5)
```

## Customization

- Edit `docker-compose.yml` to add environment variables or ports
- Modify `Dockerfile.jupyter` to add more Python packages
- Place your data files in the `data/` directory
- Save your notebooks in the `notebooks/` directory

## Backup

Your work is automatically saved in local directories. For extra safety:
```bash
tar -czf backup-$(date +%Y%m%d).tar.gz notebooks/ data/ models/
```
