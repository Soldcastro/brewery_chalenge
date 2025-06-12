"""
# Brewery Challenge - Process Silver Layer
"""
# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Process Silver") \
    .getOrCreate()

# Defines the functions to process the silver data
def get_bronze_data(spark_ctx, read_path) -> DataFrame:
    """
    Reads the bronze data from the specified path.
    """
    try:
        bronze_df = spark_ctx.read.json(read_path, multiLine=True)
    except IOError as e:
        print(f"Error reading JSON file: {e}")
        raise

    # Check if the DataFrame is empty  
    if bronze_df.isEmpty():
        print("No data found in the bronze layer.")
        spark_ctx.stop()
        raise ValueError("Empty DataFrame: No data to process.")
    return bronze_df

def transform_bronze_to_silver(bronze_df) -> DataFrame:
    """
    Transforms the bronze DataFrame to the silver format.
    
    :param bronze_df: DataFrame containing the bronze data
    :return: Transformed DataFrame in silver format
    """
    return bronze_df.select(
        col("id").alias("brewery_id"),
        col("name").alias("brewery_name"),
        "brewery_type",
        "city",
        "state",
        "country"
    )

def write_silver_data(silver_df, write_path) -> None:
    """
    Writes the silver DataFrame to the specified path.
    
    :param silver_df: DataFrame containing the silver data
    :param write_path: Path to write the silver data
    """
    try:
        silver_df.write.mode("overwrite").partitionBy('country', 'brewery_type').parquet(write_path)
    except IOError as e:
        print(f"Error writing DataFrame to silver layer: {e}")

def main():
    """
    Main function to process the silver layer of the brewery data.
    It reads the bronze data, transforms it to silver format, and writes it to the specified path.
    """
    read_path = "s3a://datalake/bronze/bronze_breweries.json"
    write_path = "s3a://datalake/silver/breweries"

    # Read bronze data
    bronze_df = get_bronze_data(spark, read_path)
     
    # Delete the existing silver data if it exists
    if os.path.exists(write_path):
        os.system(f"rm -rf {write_path}")
        

    # Transform to silver format
    silver_df = transform_bronze_to_silver(bronze_df)

    # Write silver data
    write_silver_data(silver_df, write_path)

    spark.stop()

    print("Silver data processing completed successfully.")

if __name__ == "__main__":
    main()
