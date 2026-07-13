"""
Goal: ETL-Process raw files in GCS Bucket using Spark

Description: Sent to GCP Dataproc to run as a Spark job.

Key Features:
- Validates file availability in GCS Bucket
- Read raw files from GCS Bucket as Spark DataFrames
- Validates file schema
- Processes raw files using Spark transformations
- Writes processed files to GCS Bucket

Pre-requisites:
Upload required files to GCS Bucket:
GCS_PATH="gs://blent_spark_bucket5"
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py credentials.json"
for PATH_i in $FILEPATHS; do
    gcloud storage cp ./$PATH_i $GCS_PATH/$PATH_i
done

Usage:
gcloud dataproc jobs submit pyspark $GCS_PATH/src/job_spark.py \
--cluster=main-cluster --region=us-central1 \
--files="$GCS_PATH/credentials.json,$GCS_PATH/src/config.ini" \
--py-files="$GCS_PATH/src/lib_common.py" -- \
--DATE_START="2019-10-01 00:00:00" --DATE_END="2019-10-16 00:00:00"

For future use, it's possible to pass env variables
--properties="spark.yarn.appMasterEnv.ENV_VARIABLE=env_value" \
"""

import logging
import configparser
import os
import sys  
from gcsfs import GCSFileSystem
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType
from lib_common import GCS, get_gcs_dict, setup_logging, parse_args, \
                        list_files_to_process, search_file_in_bucket


# ____________________ Variables Initialization _____________________


# Read configuration file
# Argument --files places a copy in Job's working dir
config = configparser.ConfigParser()
# config.read("src/config.ini")  # Debug local execution
config.read("config.ini")  # Production remote execution


# Configure Logging
logging.basicConfig(
    level=config.get("LOGGING", "LOG_LEVEL"),
    format=config.get("LOGGING", "LOG_FORMAT")
)


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
        .config(  # __________ AWS & GCS Storage _________________________
            "spark.jars.packages",  # Spark syntax for list = "...,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.8,"
            "org.apache.hadoop:hadoop-aws:3.4.2"
        ) \
        .config(  # __________ GCS Bucket ________________________________
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
        .config(  # __________ General ___________________________________
            "spark.ui.showConsoleProgress",
            "false"
        ) \
        .config(  # use old parser that accepts time suffix "UTC" in raw files
            "spark.sql.legacy.timeParserPolicy",
            "LEGACY"
        ) \
        .getOrCreate()

    # Set Spark's internal log level to ERROR only
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def log_count(sdf_in, step_name):
    logging.info(f"✅ {sdf_in.count()} rows after {step_name}.")


def get_schema():

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


def extract(gcs, spark, filenames):
    """
    2. Ingestion: Read raw data
    input_path: 1 file or list in local storage, AWS S3, GCS Buckets
    """

    gcs.GCS_ABS_RAW_DIR = f"{gcs.BASE_DIR}/{gcs.REL_RAW_DIR}"
    filepaths = [f"{gcs.GCS_ABS_RAW_DIR}/{filename}" for filename in filenames]
    logging.info(f"📥 Extracting data from: {filepaths}...")

    spark_df = spark.read.csv(
        filepaths,
        header=True,
        inferSchema=False
    )

    # ToDo: remove after debugging
    spark_df = spark_df.limit(1000)

    log_count(spark_df, "extraction")

    return spark_df


# def transform(sdf, date_start=None, date_stop=None):
def transform(sdf, args):
    """
    3. Transformation: Feature Engineering
    """

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
                f.countDistinct(
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
                # f.col("temp_end_dt").cast("long") - \
                # f.col("temp_start_dt").cast("long")
                f.unix_timestamp("temp_end_dt") - f.unix_timestamp("temp_start_dt")
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

    # Body of transform()

    logging.info("⚙️ Transforming data...")
    date_start, date_stop = args.DATE_START, args.DATE_END

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

    file_sys = GCSFileSystem()
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


def main(config):

    setup_logging(config)
    logging.info("🚀 Starting Spark Job")

    # 1. Parse arguments using defaults from config.ini
    gcs_dict = get_gcs_dict(config)
    gcs = GCS(**gcs_dict)
    args = parse_args(config, gcs.BASE_DIR, "Spark Job")
    gcs.REL_RAW_DIR = args.REL_RAW_DIR

    # 2. Verify presence of source files in Bucket
    selected_files = list_files_to_process(args, config)
    for filename in selected_files:
        file_found_in_bucket = search_file_in_bucket(gcs, filename)
        if not file_found_in_bucket:
            logging.error("❌ Source files aren't available.")
            sys.exit(1)
    logging.info(f"✅ Source files available in Bucket:\n{gcs.filepaths}")

    # 3. Create Spark Session
    spark = create_spark_session(gcs.PROJECT_ID)

    # 4. ETL Job
    logging.info("🚀 Starting ETL Job...")
    exception_caught = 0
    try:
        # 4.1. Extract
        sdf_fact_table = extract(gcs, spark, selected_files)

        # 4.2. Transform
        # sdf_feature_table = transform(sdf_fact_table, args.DATE_START, args.DATE_END)
        sdf_feature_table = transform(sdf_fact_table, args)

        # 4.3. Load
        load(sdf_feature_table, args.DESTINATION)

        logging.info("🏁 ETL Job Finished.")

    except Exception as e:
        logging.error(f"❌ ETL Job Failed: {e}")
        exception_caught = 1

    finally:
        # 4. Close spark session
        spark.stop()
        sys.exit(exception_caught)


if __name__ == "__main__":
    main(config)
