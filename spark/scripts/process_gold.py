# import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the paths for reading from the bronze layer and writing to the silver layer
read_path = "/datalake/silver/breweries"
write_path = "/datalake/gold/aggregated_breweries"

# Create a Spark session
spark = SparkSession.builder \
    .appName("Process Gold") \
    .getOrCreate()

# Read the Parquet file from the silver layer
try:
    silver_df = spark.read.parquet(read_path)
except Exception as e:
    print(f"Error reading Parquet file: {e}")
    spark.stop()
    raise
# Check if the DataFrame is empty
if silver_df.isEmpty():
    print("No data found in the silver layer.")
    spark.stop()
    raise ValueError("Empty DataFrame: No data to process.")
# Aggregate the data by country and brewery_type
gold_df = silver_df.groupBy("country", "brewery_type") \
    .count() \
    .withColumnRenamed("count", "brewery_count")
# Write the aggregated DataFrame to the gold layer in Parquet format
try:
    gold_df.write.mode("overwrite").parquet(write_path)
except Exception as e:
    print(f"Error writing DataFrame to gold layer: {e}")
    spark.stop()
    raise
# Stop the Spark session
spark.stop()
# Print a message indicating successful processing
print("Data processed and written to gold layer successfully.")