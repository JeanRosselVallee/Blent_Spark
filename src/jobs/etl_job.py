"""
# Spark Transformation Prototype
This notebook demonstrates a basic PySpark ETL pipeline:
    - Extraction from Google Cloud Storage
    - Transformation
    - Export to GCS.
Pre-requisites:
    - GCP project with GCS bucket and data
    - run setup_env_gcs.sh to set env for Python, Spark & GCS
Command-line arguments
    - SOURCE: GCS path to input CSV (default from env)
    - DESTINATION: GCS path to output directory (default from env)
    - date filters : --DATE_START, --DATE_END
"""

import os
import logging
import argparse
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from gcsfs import GCSFileSystem
from datetime import datetime
from lib_common import setup_logging


def load_config():
    """Loads configuration from environment variables."""
    load_dotenv()
    config = {
        "GC_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "INPUT_PATH": os.getenv("INPUT_PATH"),
        "OUTPUT_PATH": os.getenv("OUTPUT_PATH"),
        "RAW_FILE_PATH": os.getenv("RAW_FILE_PATH"),
    }
    return config


def parse_args(defaults):
    """Parses command line arguments with environment-based defaults."""

    # Get default values
    default_input = defaults.get("RAW_FILE_PATH")
    default_output = defaults.get("OUTPUT_PATH")

    parser = argparse.ArgumentParser(
        description="Spark ETL Job with GCS"
    )
    parser.add_argument(
        "--SOURCE",
        type=str,
        default=default_input,
        help=f"GCS path to input CSV (default: {default_input})"
    )
    parser.add_argument(
        "--DESTINATION",
        type=str,
        default=default_output,
        help=f"GCS path to output directory (default: {default_output})"
    )
    parser.add_argument(
        "--DATE_START",
        type=str,
        help="Start date (YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument(
        "--DATE_END",
        type=str,
        help="End date (YYYY-MM-DD HH:MM:SS)"
    )
    parse_args = parser.parse_args()

    # Log args
    args_dict = vars(parse_args)
    logging.info("🔧 Active Parameters:")
    for key, val in args_dict.items():
        logging.info(f"  • {key}: {val}")

    return parse_args


def check_dates_and_source(args):

    # Validate dates
    date_format = "%Y-%m-%d %H:%M:%S"
    start, end = args.DATE_START, args.DATE_END
    if start or end:
        try:
            start_dt = datetime.strptime(start, date_format)
            end_dt = datetime.strptime(end, date_format)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")
        if start_dt > end_dt:
            raise ValueError("DATE_START must be before DATE_END.")

    # Validate source
    input_folder = args.SOURCE
    file_sys = GCSFileSystem()
    input_files_are_missing = not file_sys.exists(input_folder)
    if input_files_are_missing:
        error_message = f"Input file(s) not found at: {input_folder}"
        raise FileNotFoundError(error_message)


def create_spark_session(project_id):
    """
    Initializes a Spark Session with GCS support using ADC.
    Spark requires a connector to access the large files stored in GCS.
    """

    # Get credentials
    credentials_path = "~/.config/gcloud/application_default_credentials.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
        os.path.expanduser(credentials_path)
    
    spark = SparkSession.builder \
        .appName("GCS_ETL_Job") \
        .config(
                "spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.8") \
        .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config(
            "spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT") \
        .config(
            "spark.hadoop.fs.gs.project.id", project_id) \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    # Set Spark's internal log level to ERROR only
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def extract(spark, input_path):
    """
    2. Ingestion: Read raw data
    """
    logging.info(f"📥 Extracting data from: {input_path}...")
    df = spark.read.csv(
        input_path, 
        header=True, 
        inferSchema=True
    )
    logging.info(f"✅ Extracted {df.count()} rows.")
    return df


def transform(df, date_start=None, date_stop=None):
    """
    3. Transformation: Filter for 'view' events and select columns
    """
    logging.info("⚙️ Transforming data...")
    transformed_df = df.filter(col("event_type") == "view")
    
    # Filter dates if provided
    if date_start:
        logging.info(f"📅 Filtering data from: {date_start}")
        transformed_df = transformed_df.filter(col("event_time") >= date_start)
    if date_stop:
        logging.info(f"📅 Filtering data until: {date_stop}")
        transformed_df = transformed_df.filter(col("event_time") <= date_stop)

    transformed_df = transformed_df \
        .select("event_time", "brand", "price", "user_id") \
        .limit(1000) # Limit for prototype validation
    return transformed_df


def load(spark_df, output_path):
    """
    4. Load: Write to processed data folder and verify
    """
    logging.info(f"📤 Loading data to: {output_path}...")
    spark_df \
        .write.mode("overwrite") \
        .csv(
            output_path, 
            header=True
        )
    # Check Load Execution
    file_sys = GCSFileSystem()
    load_success = file_sys.exists(f"{output_path}/_SUCCESS")
    if not load_success:
        logging.error("⚠️ Data load failed.")
        return

    # Get output files
    logging.info("✅ Data loaded successfully to GCS.")
    folder_contents = file_sys.ls(output_path)
    logging.info("📄 Generated part files:")
    for file in folder_contents:
        if file.endswith(".csv"):
            logging.info(f"   • gs://{file}")


def main():

    # 1. Get env variables
    config = load_config()

    # 2. Setup logging
    setup_logging()

    # 3. Parse args using config as defaults
    args = parse_args(config)

    # 4. Check if source files are missing
    try:
        check_dates_and_source(args)
    except Exception as e:
        logging.error(f"❌ {e}")
        return

    # 5. Create Spark Session
    spark = create_spark_session(config["GC_PROJECT_ID"])

    # 6. ETL Job
    logging.info("🚀 Starting ETL Job...")
    try:
        # 6.1. Extract
        raw_data = extract(spark, args.SOURCE)

        # 6.2. Transform
        processed_data = transform(raw_data, args.DATE_START, args.DATE_END)

        # 6.3. Load
        load(processed_data, args.DESTINATION)

        logging.info("🏁 ETL Job Finished.")

    except Exception as e:
        logging.error(f"❌ ETL Job Failed: {e}")

    finally:
        # 7. Close spark session
        spark.stop()


if __name__ == "__main__":
    main()
