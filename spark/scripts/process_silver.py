# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the paths for reading from the bronze layer and writing to the silver layer
read_path = "/datalake/bronze/raw_breweries.json"
write_path = "/datalake/silver/breweries"

# Create a Spark session
spark = SparkSession.builder \
    .appName("Process Silver") \
    .getOrCreate()

# Read the JSON file from the bronze layer
try:
    bronze_df = spark.read.json(read_path, multiLine=True)
except Exception as e:
    print(f"Error reading JSON file: {e}")
    spark.stop()
    raise

# Check if the DataFrame is empty
if bronze_df.isEmpty():
    print("No data found in the bronze layer.")
    spark.stop()
    raise ValueError("Empty DataFrame: No data to process.")

# Select the required columns and rename them
silver_df = bronze_df.select(
    col("id").alias("brewery_id"),
    col("name").alias("brewery_name"),
    "brewery_type",
    "city",
    "state",
    "country"
)
# Write the DataFrame to the silver layer in Parquet format partitioned by country and brewery_type
try:
    silver_df.write.mode("overwrite").partitionBy('country','brewery_type').parquet(write_path)
except Exception as e:
    print(f"Error writing DataFrame to silver layer: {e}")
    spark.stop()
    raise
# Stop the Spark session
spark.stop()

# Print a message indicating successful processing
print("Data processed and written to silver layer successfully.")