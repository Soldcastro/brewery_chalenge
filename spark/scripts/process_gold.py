"""
# Brewery Challenge - Process Gold Layer
"""

# import necessary libraries
from pyspark.sql import SparkSession, DataFrame

# create a Spark session
spark = SparkSession.builder \
    .appName("Process Gold") \
    .getOrCreate()

def get_silver_data(read_path: str) -> DataFrame:
    """
    Function to read silver data from a given path.
    """
    try:
        silver_df = spark.read.parquet(read_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        raise
    if silver_df.isEmpty():
        print("No data found in the silver layer.")
        spark.stop()
        raise ValueError("Empty DataFrame: No data to process.")
    return silver_df

def process_gold(silver_df: DataFrame) -> DataFrame:
    """
    Function to process silver data and write it to the gold layer.
    """
    try:
        # Makes aggregation to count breweries by country and type
        gold_df = silver_df.groupBy("country", "brewery_type") \
            .count() \
            .withColumnRenamed("count", "brewery_count")
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise
    return gold_df

def write_gold_data(gold_df: DataFrame, write_path: str) -> None:
    """
    Function to write the processed gold data to a given path.
    """
    try:
        gold_df.write.mode("overwrite").parquet(write_path)
        print(f"Gold data written successfully to {write_path}")
    except Exception as e:
        print(f"Error writing Parquet file: {e}")
        raise

def main():
    """
    Main function to orchestrate the reading, processing, and writing of data.
    """
    read_path = "s3a://datalake/silver/breweries"
    write_path = "s3a://datalake/gold/aggregated_breweries"

    silver_df = get_silver_data(read_path)
    gold_df = process_gold(silver_df)
    write_gold_data(gold_df, write_path)

    spark.stop()

if __name__ == "__main__":
    main()
    