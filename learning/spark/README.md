# Spark Learning Environment

Local PySpark development environment with a Docker-based Spark cluster.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker and Docker Compose
- Java 21 (for local PySpark)

## Setup

1. Install Python dependencies:
   ```bash
   uv sync --all-extras
   ```

2. Start the Spark cluster:
   ```bash
   docker compose up -d
   ```

3. Verify the cluster is running:
   - Master UI: http://localhost:8080
   - Worker UI: http://localhost:8081

## Usage

Run PySpark scripts locally that connect to the Docker cluster:

```bash
uv run python apps/example.py
```

### Connecting to the Cluster

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Your code here

spark.stop()
```

## Project Structure

```
├── apps/           # PySpark scripts
├── notebooks/      # Jupyter notebooks
├── docker-compose.yml
├── pyproject.toml
└── uv.lock
```

## Cluster Management

```bash
# Start cluster
docker compose up -d

# Stop cluster
docker compose down

# View logs
docker compose logs -f
```
