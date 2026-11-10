from pyspark.sql import SparkSession

# Create a local Spark session
spark = (
    SparkSession.builder
    .appName("YottaFlexDataQualityPipeline")
    .master("local[*]")
    .getOrCreate()
)

# Reduce unnecessary Spark logs
spark.sparkContext.setLogLevel("ERROR")

# Read transaction JSON file
transactions_df = spark.read.json(
    "data/input/transactions.json"
)

# Display schema
print("TRANSACTION SCHEMA")
transactions_df.printSchema()

# Display records
print("TRANSACTION DATA")
transactions_df.show(truncate=False)

# Count records
print("TOTAL RECORDS:", transactions_df.count())

# Stop Spark
spark.stop()