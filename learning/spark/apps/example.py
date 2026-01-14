from pyspark.sql import SparkSession

# Connect to Docker Spark cluster
spark = SparkSession.builder \
    .appName("LocalDev") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Test it works
df = spark.range(100)
print(f"Count: {df.count()}")

spark.stop()
