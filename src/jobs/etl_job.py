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
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from gcsfs import GCSFileSystem
from datetime import datetime
from src.jobs.lib_common import setup_logging


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


def log_count(sdf_in, step_name):
    logging.info(f"✅ {sdf_in.count()} rows after {step_name}.")


def extract(spark, input_path):
    """
    2. Ingestion: Read raw data
    """
    logging.info(f"📥 Extracting data from: {input_path}...")
    spark_df = spark.read.csv(
        input_path, 
        header=True, 
        inferSchema=True
    )
    # logging.info(f"✅ Extracted {spark_df.count()} rows.")
    log_count(spark_df, "extraction")

    return spark_df


def transform(sdf, date_start=None, date_stop=None):
    """
    3. Transformation: Feature Engineering
    """
    logging.info("⚙️ Transforming data...")

    def clean_data(sdf_in):
        """
        Data Cleaning: Remove sessions with multiple user_ids (from notebook 3.1.3)
        """
        logging.info("🧹 Cleaning data: removing sessions with duplicate user IDs...")
        sdf_dupl_sessions = sdf_in.select("user_session", "user_id") \
            .distinct() \
            .groupBy("user_session") \
            .count() \
            .filter(f.col("count") > 1)
        
        sdf_out = sdf_in.join(sdf_dupl_sessions, "user_session", "left_anti")
        log_count(sdf_out, "cleaning")

        return sdf_out

    def get_features_per_product(sdf_in):
        """
        a) Attributes and features per product
        """
        was_purchased = (f.col("event_type") == "purchase")
        was_viewed = (f.col("event_type") == "view")

        sdf_out = sdf_in \
            .filter(f.col("event_type").isin(["purchase", "view"])) \
            .groupBy("product_id", "category_code", "brand", 
                     "price", "user_session") \
            .agg(
                f.max(was_purchased.cast("int")).alias("purchased"),
                f.sum(was_viewed.cast("int")).alias("num_views_product")
            )
        log_count(sdf_out, "calculation of features per product")

        return sdf_out

    def get_features_per_session(sdf_in):
        """
        b) Features per session
        """
        was_viewed = (f.col("event_type") == "view")

        sdf_per_session = sdf_in \
            .groupBy("user_session", "user_id") \
            .agg(
                f.count_distinct(
                    f.when(was_viewed, f.col("product_id"))
                ).alias("num_views_session"),
                f.min("event_time").alias("temp_start_dt"),
                f.max("event_time").alias("temp_end_dt")
            )

        # Window for num_prev_sessions
        window_user = Window \
            .partitionBy("user_id") \
            .orderBy("temp_start_dt") \
            .rowsBetween(Window.unboundedPreceding, -1)

        sdf_out = sdf_per_session \
            .withColumn(
                "start_time", 
                f.date_format(f.col("temp_start_dt"), "HH:mm")
            ) \
            .withColumn(
                "start_weekday", 
                f.date_format(f.col("temp_start_dt"), "EEEE")
            ) \
            .withColumn(
                "duration", 
                f.col("temp_end_dt").cast("long") - \
                f.col("temp_start_dt").cast("long")
            ) \
            .withColumn(
                "num_prev_sessions", 
                f.count("user_session").over(window_user)
            )

        log_count(sdf_out, "calculation of features per session")

        return sdf_out

    def get_features_per_product_per_session(sdf_per_product, sdf_per_session):
        """
        c) Features per product per session
        """
        sdf_features = sdf_per_product.join(
            sdf_per_session,
            "user_session",
            "inner"
        )

        # Window for num_prev_product_views
        period_before_current_session = Window \
            .partitionBy("product_id", "user_id") \
            .orderBy("temp_start_dt") \
            .rowsBetween(Window.unboundedPreceding, -1)

        sdf_out = sdf_features.withColumn(
            "num_prev_product_views",
            f.coalesce(
                f.sum("num_views_product").over(period_before_current_session), 
                f.lit(0)
            )
        )

        log_count(sdf_out, "calculation of features per product & per session")

        return sdf_out


    # 1. Clean Data
    sdf = clean_data(sdf)

    # 2. Filter dates if arguments provided
    if date_start:
        logging.info(f"📅 Filtering data from: {date_start}")
        sdf = sdf.filter(f.col("event_time") >= date_start)
    if date_stop:
        logging.info(f"📅 Filtering data until: {date_stop}")
        sdf = sdf.filter(f.col("event_time") <= date_stop)

    # Optimization: Cache sdf since it's used as a source for two branches
    sdf.persist()

    # 3. Get Features
    sdf_per_product = get_features_per_product(sdf)
    sdf_per_session = get_features_per_session(sdf)
    sdf_features = get_features_per_product_per_session(
        sdf_per_product, 
        sdf_per_session
    )
    
    # 4. Final selection and cleanup
    transformed_df = sdf_features \
        .drop("temp_start_dt", "temp_end_dt")  #, "user_id") \
        # .limit(1000) # ToDo: To comment after prototype validation
    
    log_count(transformed_df, "transformation")

    return transformed_df


def load(sdf, output_path):
    """
    4. Load: Write to processed data folder and verify
    """
    logging.info(f"📤 Loading data to: {output_path}...")
    sdf \
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
        sdf_fact_table = extract(spark, args.SOURCE)

        # 6.2. Transform
        sdf_feature_table = transform(sdf_fact_table, args.DATE_START, args.DATE_END)

        # 6.3. Load
        load(sdf_feature_table, args.DESTINATION)

        logging.info("🏁 ETL Job Finished.")

    except Exception as e:
        logging.error(f"❌ ETL Job Failed: {e}")

    finally:
        # 7. Close spark session
        spark.stop()


if __name__ == "__main__":
    main()
