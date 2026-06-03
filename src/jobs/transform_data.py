from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse


def create_spark_session(app_name):
    """Initializes a Spark Session."""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def transform(input_path, output_path, date_start=None, date_stop=None):
    """
    Main transformation logic.
    1. Ingestion
    2. Transformation (with date filtering)
    3. Export
    """
    spark = create_spark_session("DataEng_ML_Spark_Job")
    
    # 1. Ingestion
    print(f"Reading data from {input_path}...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 2. Transformation
    # Rows are filtered before columns to optimize performance

    # Filter-in 'view' events 
    print("Transforming data...")
    transformed_df = df.filter(col("event_type") == "view")
    
    # Filter dates if provided
    if date_start:
        print(f"Filtering data from: {date_start}")
        transformed_df = transformed_df.filter(col("event_time") >= date_start)
    if date_stop:
        print(f"Filtering data until: {date_stop}")
        transformed_df = transformed_df.filter(col("event_time") <= date_stop)

    # Select columns
    transformed_df = transformed_df \
        .select("event_time", "brand", "price", "user_id") \
        .limit(1000)  # ToDo: remove temporary limit in production.

    # 3. Export
    print(f"Writing data to {output_path}...")
    transformed_df.write.mode("overwrite").csv(output_path, header=True)
    print("Job completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Data Transformation Job"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/raw/sample.csv",
        help="Path to input CSV"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/job_output",
        help="Path to output CSV"
    )
    parser.add_argument(
        "--date-start",
        type=str,
        help="Start date (YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument(
        "--date-stop",
        type=str,
        help="End date (YYYY-MM-DD HH:MM:SS)"
    )
    
    args = parser.parse_args()
    
    transform(args.input, args.output, args.date_start, args.date_stop)
