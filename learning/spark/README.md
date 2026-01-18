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
├── models/         # Pydantic data models
│   ├── base.py     # SparkModel base class
│   └── examples/   # Example models
├── notebooks/      # Jupyter notebooks
├── docker-compose.yml
├── pyproject.toml
└── uv.lock
```

## Pydantic Integration

The project includes Pydantic v2 for data validation with built-in Spark DataFrame conversion via the `SparkModel` base class.

### Creating Models

```python
from typing import Annotated
from pydantic import Field
from models import SparkModel

class Employee(SparkModel):
    name: Annotated[str, Field(min_length=1)]
    department: str
    salary: Annotated[int, Field(gt=0)]
```

### Converting Between Pydantic and DataFrames

```python
from models.examples.employee import Employee

# Create validated data
employees = [
    Employee(name="Alice", department="Engineering", salary=75000),
    Employee(name="Bob", department="Sales", salary=65000),
]

# Convert to DataFrame using class method
df = Employee.to_dataframe(spark, employees)

# Get the Spark schema
schema = Employee.spark_schema()

# Convert back to models
recovered = Employee.from_dataframe(df)
```

### Running the Pydantic Example

```bash
uv run python apps/pydantic_example.py
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
