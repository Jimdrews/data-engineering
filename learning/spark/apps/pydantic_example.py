"""Example demonstrating Pydantic integration with PySpark."""

import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pydantic import ValidationError
from pyspark.sql import SparkSession

from models.examples.employee import Employee

# Connect to Spark cluster
spark = SparkSession.builder \
    .appName("PydanticExample") \
    .master("spark://localhost:7077") \
    .getOrCreate()

print("=" * 60)
print("Pydantic + PySpark Integration Example")
print("=" * 60)

# Create validated employee data using Pydantic
print("\n1. Creating validated employees with Pydantic:")
employees = [
    Employee(name="Alice", department="Engineering", salary=75000),
    Employee(name="Bob", department="Engineering", salary=80000),
    Employee(name="Charlie", department="Sales", salary=65000),
    Employee(name="Diana", department="Sales", salary=70000),
    Employee(name="Eve", department="Marketing", salary=60000),
]
for emp in employees:
    print(f"   {emp}")

# Convert to DataFrame
print("\n2. Converting Pydantic models to DataFrame:")
df = Employee.to_dataframe(spark, employees)
df.show()

# Perform Spark operations
print("3. Spark operations - Filter salary > 65000:")
df.filter(df.salary > 65000).show()

print("4. Spark operations - Average salary by department:")
df.groupBy("department").avg("salary").show()

# Convert back to Pydantic models
print("5. Converting DataFrame back to Pydantic models:")
recovered = Employee.from_dataframe(df)
for emp in recovered:
    print(f"   {emp}")

# Demonstrate validation error handling
print("\n6. Validation error handling:")
print("   Attempting to create employee with invalid data...")
try:
    invalid = Employee(name="", department="HR", salary=-1000)
except ValidationError as e:
    print("Validation failed (as expected):")
    for error in e.errors():
        print(f"     - {error['loc'][0]}: {error['msg']}")

print("\n" + "=" * 60)
print("Example complete!")
print("=" * 60)

spark.stop()
