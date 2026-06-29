"""
# Spark Transformation Prototype
This notebook demonstrates a basic PySpark ETL pipeline:
    - Extraction from Google Cloud Storage
    - Transformation
    - Export to GCS.
Pre-requisites:
    - GCP project with GCS bucket and data
    - run setup_env_gcs.sh to set env for Python, Spark & GCS
Syntax : python -m src.etl_job
Command-line arguments
    - SOURCE: GCS path to input CSV (default from env)
    - DESTINATION: GCS path to output directory (default from env)
    - date filters : --DATE_START, --DATE_END
"""

import os
import logging
import argparse
import fsspec
import subprocess  # execute Shell commands
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
# from gcsfs import GCSFileSystem
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.lib_common import setup_logging


def load_config():
    """Loads configuration from environment variables."""
    load_dotenv()
    config = {
        "GC_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "INPUT_LIST": os.getenv("INPUT_LIST"),
        "INPUT_PATH": os.getenv("INPUT_PATH"),
        "OUTPUT_PATH": os.getenv("OUTPUT_PATH"),
        "DATE_START": os.getenv("DATE_START"),
        "DATE_END": os.getenv("DATE_END"),
        #"RAW_FILE_PATH": os.getenv("RAW_FILE_PATH"),
    }
    return config


def parse_args(defaults):
    """Parses command line arguments with environment-based defaults."""

    # Get default values
    default_input_list = defaults.get("INPUT_LIST")
    # default_input_path = defaults.get("INPUT_PATH")
    default_output = defaults.get("OUTPUT_PATH")
    default_start = defaults.get("DATE_START")
    default_end = defaults.get("DATE_END")

    parser = argparse.ArgumentParser(
        description="Spark ETL Job with GCS"
    )
    parser.add_argument(
        "--SOURCE",
        type=str,
        default=default_input_list,
        help=f"List of S3 URLs of sources (default: {default_input_list})"
    )
    # parser.add_argument(
    #     "--SOURCE",
    #     type=str,
    #     default=default_input,
    #     help=f"GCS path to input CSV (default: {default_input})"
    # )
    parser.add_argument(
        "--DESTINATION",
        type=str,
        default=default_output,
        help=f"GCS path to output directory (default: {default_output})"
    )
    parser.add_argument(
        "--DATE_START",
        type=str,
        default=default_start,
        help=f"Start date (YYYY-MM-DD HH:MM:SS default: {default_start})"
    )
    parser.add_argument(
        "--DATE_END",
        type=str,
        default=default_end,
        help=f"End date (YYYY-MM-DD HH:MM:SS default: {default_end})"
    )
    parse_args = parser.parse_args()

    # Log args
    args_dict = vars(parse_args)
    logging.info("🔧 Active Parameters:")
    for key, val in args_dict.items():
        logging.info(f"  • {key}: {val}")

    return parse_args


def check_dates(args):
    # Check dates
    date_format = "%Y-%m-%d %H:%M:%S"
    try:
        start_dt = datetime.strptime(args.DATE_START, date_format)
        end_dt = datetime.strptime(args.DATE_END, date_format)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")
    if start_dt > end_dt:
        raise ValueError("DATE_START must be before DATE_END.")
    return start_dt, end_dt


def copy_sources_to_bucket(args, start_dt, end_dt, gcs_input_dir):

    # 1. Get selected months from date arguments
    months = []
    temp_dt = start_dt
    while temp_dt <= end_dt:
        months.append(temp_dt.strftime("%Y-%b"))  # Ex. 2026-Jan
        temp_dt += relativedelta(months=1)  # increments to next month

    # 2. Select URLs in text file
    URLs_list_file = args.SOURCE
    with open(URLs_list_file) as file_stream:
        urls = [line.strip() for line in file_stream]  # remove \n
    # Scan URL list once
    selected_urls = [u for u in urls if any(m in u for m in months)]

    # 3. Check sources cover whole period
    if len(selected_urls) < len(months):
        raise ValueError("Missing source URLs.")

    # 4. List missing sources in Bucket
    missing_sources_in_gcs, available_sources_in_gcs = [], []
    for url in selected_urls:
        # Check source is available in Bucket
        source_basename = url.split("/")[-1]  # Get basename in URL
        gcs_file_path = f"{gcs_input_dir}/{source_basename}"
        # ToDo

        # Check source is available in S3
        # ToDo
        missing_sources_in_gcs.append((source_basename, url, gcs_file_path))
        available_sources_in_gcs.append((source_basename, url, gcs_file_path))

    # 5. Copy missing sources to Bucket
    temp_dir = "/tmp"
    for basename, url, gcs_file_path in missing_sources_in_gcs:
        temp_path = f"{temp_dir}/{basename}"
        try:
            # File Download to Temporary folder
            logging.info(f"🔄 Downloading source {url} to {temp_path}...")
            subprocess.run(
                ["wget", "--progress=dot:giga", "-O", temp_path, url],
                check=True  # raises CalledProcessError on failure
            )
            # File Upload to Bucket
            logging.info(f"🔄 Uploading {temp_path} to {gcs_file_path}...")
            subprocess.run(
                ["gcloud", "storage", "cp", temp_path, gcs_file_path],
                check=True  # raises CalledProcessError on failure
            )
            logging.info("✅ Copy complete!")
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(f"❌ Copy Failed: {e}")
        finally:
            # Remove temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"🧹 Temporary file {temp_path} removed from /tmp")

    return available_sources_in_gcs

    # input_folder = args.SOURCE
    # file_sys_name, source_path = input_folder.split("://")

    # # Set anonymous token for AWS S3
    # storage_opts = {"anon": True} if file_sys_name == "s3" else {}

    # # Set file system
    # file_sys = fsspec.filesystem(file_sys_name, **storage_opts)

    # if not file_sys.exists(source_path):
    #     raise FileNotFoundError(
    #         f"Source file(s) not found at: {input_folder}"
    #     )

    # file_sys = GCSFileSystem()
    # input_files_are_missing = not file_sys.exists(input_folder)
    # if input_files_are_missing:
    #     error_message = f"Input file(s) not found at: {input_folder}"
    #     raise FileNotFoundError(error_message)


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
        .appName("AWS_to_GCS_ETL_Job") \
        .config(  # AWS & GCS Storage ===============================
            "spark.jars.packages",  # Spark syntax for list = "...,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.8,"
            "org.apache.hadoop:hadoop-aws:3.4.2"
        ) \
        .config(  # GCS Bucket ====================================
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        ) \
        .config(
            "spark.hadoop.fs.gs.auth.type",
            "APPLICATION_DEFAULT"
        ) \
        .config(
            "spark.hadoop.fs.gs.project.id",
            project_id
        ) \
        .config(  # AWS S3 ==========================================
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        ) \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        ) \
        .config(  # General =========================================
            "spark.ui.showConsoleProgress",
            "false"
        ) \
        .getOrCreate()

        # .config(  # Force Hadoop 3.4+ to skip checking bucket structure entirely
        #     "spark.hadoop.fs.s3a.bucket.probe", "0") \
        # .config("spark.hadoop.fs.s3a.list.version", "1") \
        # .config("spark.hadoop.fs.s3a.change.detection.source", "none") \
        # .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
        # .config(  # Prevent Spark SQL data source from checking file statuses on read initialization
        #     "spark.sql.sources.parallelPartitionDiscovery.threshold", "0") \
        # .config(  # ❌ Completely disable directory verification checks on target paths
        #     "spark.hadoop.fs.s3a.directory.marker.retention", "keep") \
        # .config(
        #     "spark.hadoop.fs.s3a.performance.flags", "mkdir,delete") \
        # .config(  # 💡 Crucial: Tell Spark not to check file/directory status via lists
        #     "spark.sql.sources.fileStatusCacheDirectoryScanThreshold", "0") \
        # .config(  # 💡 Force Spark to treat the path as a direct file and bypass folder structural listings
        #     "spark.sql.sources.fileRelationCacheTtl", "0") \
        # .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        # .config(  # Tell Hadoop S3A optimization framework to optimize for a single explicit file
        #     "spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential") \
        # .config(
        #     "spark.hadoop.fs.s3a.endpoint",
        #     "s3.eu-west-3.amazonaws.com"
        # ) \
        # .config(
        #     "spark.hadoop.fs.s3a.endpoint.region",
        #     "eu-west-3"
        # ) \

        # .config(  # GCS Public Bucket ===============================
        #     "spark.hadoop.fs.gs.user.project.lookup.mode",
        #     "NONE"  # to avoid Error on GCS billing quota for a project
        # ) \
        # .config(
        #     "spark.hadoop.fs.gs.impl.disable.cache",
        #     "true"
        # ) \
        # .config(
        #     "spark.hadoop.fs.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        # ) \
        # .config(
        #     "spark.hadoop.fs.gs.auth.type",
        #     "UNAUTHENTICATED"
        # ) \
        # .config(
        #     "spark.hadoop.fs.gs.project.id",
        #     project_id
        # ) \

    # Set Spark's internal log level to ERROR only
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def log_count(sdf_in, step_name):
    logging.info(f"✅ {sdf_in.count()} rows after {step_name}.")


def get_schema():
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True)
    ])
    return schema

"""
def get_rdd_from_s3(spark, input_path):
    import csv
    raw_rdd = spark.sparkContext.textFile(input_path)

    # Filter out header line
    header_line = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"
    data_rdd = raw_rdd.filter(lambda line: line != header_line)

    # Attach a zero-based index to every distributed line in parallel
    # and filter to keep only indices below 10,000
    limited_rdd = data_rdd.zipWithIndex() \
        .filter(lambda pair: pair[1] < 100) \
        .map(lambda pair: pair[0])

    # Parse CSV lines into lists of strings
    def parse_csv_line(line):
        return next(csv.reader([line]))

    # Parse and convert to DataFrame as normal
    parsed_rdd = limited_rdd.map(parse_csv_line)

    return parsed_rdd
"""

def extract(spark, input_path):
    """
    2. Ingestion: Read raw data
    input_path: 1 file or list in local storage, AWS S3, GCS Buckets
    """

    logging.info(f"📥 Extracting data from: {input_path}...")

    spark_df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=False
    )

    # ToDo: remove after debugging
    spark_df = spark_df.limit(1000)

    # Force Spark to bypass directory tree optimization
    # spark_df = spark.read \
    #     .format("csv") \
    #     .option("header", "true") \
    #     .option("inferSchema", "false") \
    #     .load(input_path)

    # # 3. Convert the parallelized RDD back into a standard Spark DataFrame
    # parsed_rdd = get_rdd_from_s3(spark, input_path)
    # spark_df = spark.createDataFrame(parsed_rdd, schema=get_schema())
    # # 4. Cast the columns to their correct types using Spark SQL (Parallelized)
    # spark_df = spark_df.withColumn("event_time", f.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")) \
    #     .withColumn("product_id", f.col("product_id").cast("long")) \
    #     .withColumn("category_id", f.col("category_id").cast("long")) \
    #     .withColumn("price", f.col("price").cast("double")) \
    #     .withColumn("user_id", f.col("user_id").cast("long"))

    log_count(spark_df, "extraction")

    return spark_df


def transform(sdf, date_start=None, date_stop=None):
    """
    3. Transformation: Feature Engineering
    """
    logging.info("⚙️ Transforming data...")

    def clean_data(sdf_in):
        """
        Data Cleaning
        Goal: remove sessions with multiple user_ids (cf. notebook §3.1.3)
        """
        logging.info("🧹 Cleaning data of sessions with different user IDs...")
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

    success_file = f"{output_path}/_SUCCESS"
    file_sys_name, _ = success_file.split("://")

    # Set anonymous token for AWS S3
    storage_opts = {"anon": True} if file_sys_name == "s3" else {}

    # Set file system
    file_sys = fsspec.filesystem(file_sys_name, **storage_opts)

    #file_sys = GCSFileSystem()
    load_success = file_sys.exists(success_file)
    if not load_success:
        logging.error("⚠️ Data load failed.")
        return

    # Get output files
    logging.info("✅ Data loaded successfully to GCS.")
    folder_contents = file_sys.ls(output_path)
    logging.info("📄 Generated part files:")
    for file in folder_contents:
        if file.endswith(".csv"):
            logging.info(f"   • {file}")


def main():

    # 1. Get env variables
    config = load_config()

    # 2. Setup logging
    setup_logging()

    # 3. Parse args using config as defaults
    args = parse_args(config)
    try:
        start_dt, end_dt = check_dates(args)
    except Exception as e:
        logging.error(f"❌ {e}")
        return

    # 4. Copy source files to Bucket
    source_dir = config.get("INPUT_PATH")
    copy_sources_to_bucket(args, start_dt, end_dt, source_dir)

    # 5. Create Spark Session
    spark = create_spark_session(config["GC_PROJECT_ID"])

    # 6. ETL Job
    logging.info("🚀 Starting ETL Job...")
    try:
        # 6.1. Extract
        # sdf_fact_table = extract(spark, args.SOURCE)
        sdf_fact_table = extract(spark, source_dir)

        # 6.2. Transform
        sdf_feature_table = transform(
            sdf_fact_table,
            args.DATE_START,
            args.DATE_END
        )

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
