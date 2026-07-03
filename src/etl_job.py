"""
# Spark ETL Job
Basic PySpark ETL pipeline:
    - Extraction from Google Cloud Storage
    - Transformation
    - Export to GCS.
Pre-requisites:
    - GCP project & GCS bucket.
    - Source CSV files in AWS S3
    - For a local execution, run setup_env_gcs.sh to 
        - set env for Python & GCS
        - login to GCP project
1. Local execution:
    python -m src.etl_job <arguments>
    arguments:
    - GCS path to output directory (default from env): --DESTINATION
    - date filters : --DATE_START, --DATE_END
2. Execution in GCP Dataproc cluster:
gcloud dataproc jobs submit pyspark \
    $JOB_PATH \
    --cluster=$CLUSTER \
    --region=$REGION \
    --properties="spark.yarn.appMasterEnv.BUCKET_NAME=blent_spark_bucket2" \
    --files="gs://blent_spark_bucket2/config/credentials.json"
    
"""

import os
import logging
import argparse
import time
import sys
import google.auth
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from gcsfs import GCSFileSystem
from dateutil.relativedelta import relativedelta
from google.cloud import storage
from dataclasses import dataclass
from typing import List


# Set Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="▶️ %(asctime)s: %(message)s"
)

# AWS source files' dir
AWS_URL_DIR = "https://blent-learning-user-ressources.s3.eu-west-3." + \
            "amazonaws.com/projects/9c15cb"


# GCS Storage variables

gcs_dict = {
    "PROJECT_ID": google.auth.default()[1],
    "BUCKET_NAME": os.getenv("BUCKET_NAME", "blent_spark_bucket4"),
    "INPUT_REL_DIR": "data/raw",
    "OUTPUT_REL_DIR": "data/processed",
    "storage": storage.Client(),
    "API_URL": "https://storage.googleapis.com",
    "file_paths": []
}

GCS_BASE_DIR = "gs://" + gcs_dict["BUCKET_NAME"]
gcs_dict["INPUT_ABS_DIR"] = GCS_BASE_DIR + "/" + gcs_dict["INPUT_REL_DIR"]
gcs_dict["bucket"] = gcs_dict["storage"].bucket(gcs_dict["BUCKET_NAME"])


@dataclass
class GCS:
    PROJECT_ID: str
    BUCKET_NAME: str
    bucket: storage.bucket
    INPUT_ABS_DIR: str
    INPUT_REL_DIR: str
    OUTPUT_REL_DIR: str
    storage: storage.Client
    API_URL: str
    file_paths: List[str]


gcs = GCS(**gcs_dict)

# Default argument values
defaults = {
    "DATE_START": "2019-12-16 00:00:00",
    "DATE_END": "2020-01-15 00:00:00",
    "DESTINATION": f"{GCS_BASE_DIR}/{gcs.OUTPUT_REL_DIR}"
}

# defaults["DESTINATION"] = f"{GCS_BASE_DIR}/{gcs.OUTPUT_REL_DIR}"


def parse_args(defaults):
    # Parses command line arguments with dictionary "defaults"
    deft_out_dir = defaults.get("DESTINATION")
    deft_start_date = defaults.get("DATE_START")
    deft_end_date = defaults.get("DATE_END")
    DATE_FORMAT = "YYYY-MM-DD HH:MM:SS"

    parser = argparse.ArgumentParser(
        description="Spark ETL Job with GCS"
    )
    parser.add_argument(
        "--DESTINATION",
        type=str,
        default=deft_out_dir,
        help=f"GCS path to output dir (default: {deft_out_dir})"
    )
    parser.add_argument(
        "--DATE_START",
        type=str,
        default=deft_start_date,
        help=f"Start date ({DATE_FORMAT} default: {deft_start_date})"
    )
    parser.add_argument(
        "--DATE_END",
        type=str,
        default=deft_end_date,
        help=f"End date ({DATE_FORMAT} default: {deft_end_date})"
    )
    parse_args = parser.parse_args()

    # Display arguments' values
    args_dict = vars(parse_args)
    logging.info("🔧 Active Parameters:")
    for key, val in args_dict.items():
        logging.info(f"  • {key}: {val}")

    return parse_args


def transfer_raw_files(args, gcs):
    # Goal: Transfer source files from AWS S3 to GCS Bucket via STS
    # - Check if source files are already in Bucket
    # - If not, create a TSV file with the list of pending URLs
    # - Entirely performed in the Master Node

    from datetime import datetime, timezone

    # Sub-Functions of transfer_raw_files()

    def get_input_filenames(args):

        # Convert date strings to datetime 
        date_format = "%Y-%m-%d %H:%M:%S"
        try:
            start_dt = datetime.strptime(args.DATE_START, date_format)
            end_dt = datetime.strptime(args.DATE_END, date_format)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")
        if start_dt > end_dt:
            raise ValueError("DATE_START must be before DATE_END.")

        # Get filenames covering months to process
        filenames = []
        temp_dt = start_dt
        while temp_dt <= end_dt:
            month = temp_dt.strftime("%Y-%b")
            filename = f"{month}.csv"
            filenames.append(filename)  # Ex. 2026-Jan.csv
            temp_dt += relativedelta(months=1)  # increments to next month

        return filenames

    def get_pending_and_set_expected_files(gcs, input_filenames):
        # Goal: get both,
        # - gcs_file_paths: files already available in the bucket
        # - aws_pending_urls: files "pending" or missing in the bucket

        import requests

        aws_pending_urls = []
        tsv_contents = []
        for name in input_filenames:
            # Check source is available in Bucket
            gcs_file_abs_path = f"{gcs.INPUT_ABS_DIR}/{name}"
            gcs.file_paths.append(gcs_file_abs_path)
            gcs_file_rel_path = f"{gcs.INPUT_REL_DIR}/{name}"
            gcs_file_pointer = gcs.bucket.blob(gcs_file_rel_path)
            if gcs_file_pointer.exists():
                logging.info(f"⏭️ File {name} is already in Bucket")
                continue
            else:
                logging.info(f"📝 File {name} is pending")

                # ToDo: delete this mock file
                name = "sample.csv"

                # Check source is available in S3
                url_file = f"{AWS_URL_DIR}/{name}"
                response = requests.head(
                    url_file,
                    allow_redirects=True,  # if URL points to another
                    timeout=5  # in seconds, default: waits response forever
                )
                if response.status_code != 200:
                    raise FileNotFoundError(f"❌ URL {url_file} not valid")

                aws_pending_urls.append(url_file)
                file_size = response.headers.get("Content-Length", "1000")
                tsv_contents.append(f"{url_file}\t{file_size}")

        return aws_pending_urls, tsv_contents

    def create_tsv_file(gcs, tsv_contents):
        # Create list of pending URLs as a TSV file (STS requirement)
        tsv_content = \
            "TsvHttpData-1.0\n" + "\n".join(tsv_contents) + "\n"
        #tsv_filename = "aws_pending_urls.tsv"
        tsv_filename = "tsv/aws_pending_urls.tsv"
        tsv_pointer = gcs.bucket.blob(tsv_filename)
        tsv_pointer.upload_from_string(
            tsv_content,
            content_type="text/tab-separated-values",
            predefined_acl="publicRead"
        )
        #tsv_url = f"{gcs.API_URL}/{gcs.BUCKET_NAME}/{tsv_filename}"
        tsv_url = f"{gcs.API_URL}/{gcs.BUCKET_NAME}/{tsv_filename}"
        logging.info(f"Created list of URLs to download at: {tsv_url}")
        return tsv_url

    def transfer_files_from_aws_to_gcs(gcs, tsv_url):
        from google.oauth2 import service_account

        import subprocess
        # Install Storage transfer module only in Master Node
        subprocess.check_call([
            sys.executable,  # executes Python
            "-m", "pip", "install", "--quiet",
            "google-cloud-storage-transfer==1.19.0",
            "--no-deps"  # leaves core GCS modules untouched
        ])
        from google.cloud import storage_transfer_v1

        # Define Trasfer Job
        # today = datetime.now(timezone.utc)
        # today_as_dict = \
        #     {"year": today.year, "month": today.month, "day": today.day}

        # Define Trasfer Job Name
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        job_name = f"transferJobs/project_{gcs.PROJECT_ID}_{timestamp}"

        transfer_job_definition = {
            "name": job_name,
            "project_id": gcs.PROJECT_ID,
            "status": storage_transfer_v1.TransferJob.Status.ENABLED,
            "description": "Cloud-Transfer of Raw Files",
            "logging_config": {
                "log_actions": ["FIND", "COPY", "DELETE"],
                "log_action_states": ["SUCCEEDED", "FAILED"]
            },
            "transfer_spec": {
                "http_data_source": {
                    "list_url": tsv_url  # ************* Source
                },
                "gcs_data_sink": {
                    "bucket_name": gcs.BUCKET_NAME,  # Destination
                    "path": gcs.INPUT_REL_DIR + "/"
                }
            },
            # "schedule": {  # Runs immediately
            #     "schedule_start_date": today_as_dict,
            #     "schedule_end_date": today_as_dict
            # }
        }

        # ToDo: Update these comments
        # 1. Explicitly load the credentials file at runtime
        client_credentials = service_account.Credentials \
            .from_service_account_file('credentials.json')
        # 2. Pass the credentials directly into your client
        client = storage_transfer_v1 \
            .StorageTransferServiceClient(credentials=client_credentials)
        # 3. Submit your job definition exactly like before
     
        # Run Transfer Job
        transfer = {
            "job_name": job_name,
            "client": client
            # "client": storage_transfer_v1.StorageTransferServiceClient()
        }
        # transfer["job"] = transfer["client"].create_transfer_job({
        transfer["client"].create_transfer_job({
            "transfer_job": transfer_job_definition
        })

        # transfer["job_name"] = transfer["job"].name

        # logging.info("🚀 Started File Transfer Job:" + transfer["job_name"])
        logging.info(f"🚀 Started File Transfer Job: {job_name}")
        transfer["client"] \
            .run_transfer_job({
                "job_name": job_name,
                "project_id": gcs.PROJECT_ID
            })

        return transfer

    def monitor_transfer(gcs, transfer):
        # Monitor progress of transfer operations

        from google.protobuf.json_format import MessageToDict

        logging.info("⏳ Monitoring cloud transfer... (0% Spark hardware usage)")
        operations_client = transfer["client"].transport.operations_client

        while True:

            try:  # inner try: to catch up at next operation
                # Update job status
                transfer_job_updated = transfer["client"] \
                    .get_transfer_job({
                        "job_name": transfer["job_name"],
                        "project_id": gcs.PROJECT_ID
                    })

                # Get current operation status
                current_operation_name = transfer_job_updated \
                    .latest_operation_name
                if current_operation_name is None:
                    # Phase 1. Preparation
                    logging.info("...initializing transfer...")
                else:
                    current_operation = operations_client \
                        .get_operation(current_operation_name)
                    if not current_operation.done:
                        # Phase 2. Execution
                        logging.info("⏳ Transfer in progress...")
                    else:
                        # Phase 3. Conclusion
                        logging.info("✅ Transfer completed!")
                        # Parse the operation metadata
                        if current_operation.metadata:
                            metadata_dict = MessageToDict(current_operation.metadata)
                            counters = metadata_dict.get("counters", {})
                            logging.info(f"📊 Transfer Summary: {counters}")
                        break

                time.sleep(15)  # Check every 15 seconds
            except Exception as e:
                raise ValueError(f"❌ Transfer Failed: {e}")

    # Main function body of transfer_raw_files()

    logging.info("⚙️ Preparing selected source files...")
    # 1. Get names of input files
    input_filenames = get_input_filenames(args)

    # 2. Get names of files missing in Bucket
    aws_pending_urls, tsv_contents = get_pending_and_set_expected_files(
        gcs,
        input_filenames
    )

    # 3. Transfer pending sources to Bucket via Storage Transfer Service
    if len(tsv_contents) > 0:
        tsv_url = create_tsv_file(gcs, tsv_contents)
        transfer = transfer_files_from_aws_to_gcs(gcs, tsv_url)

        # Monitor progress of transfer operations
        monitor_transfer(gcs, transfer)

    return gcs.file_paths


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

        # .config(  # 💡 Crucial: Tell Spark not to check file/directory status via lists
        #     "spark.sql.sources.fileStatusCacheDirectoryScanThreshold", "0") \


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


def main(defaults, gcs):

    project_id = gcs.PROJECT_ID

    # 1. Parse arguments using defaults
    args = parse_args(defaults)

    try:
        # 4. Make source files available in Bucket
        selected_files = transfer_raw_files(args, gcs)

    except Exception as e:
        logging.error(f"❌ Source files aren't available, \n {e}")
        sys.exit(1)

    # 2. Create Spark Session
    spark = create_spark_session(project_id)

    # 3. ETL Job
    logging.info("🚀 Starting ETL Job...")
    exception_caught = 0
    try:
        # 3.1. Extract
        sdf_fact_table = extract(
            spark,
            selected_files
        )

        # 3.2. Transform
        sdf_feature_table = transform(
            sdf_fact_table,
            args.DATE_START,
            args.DATE_END
        )

        # 3.3. Load
        load(
            sdf_feature_table,
            args.DESTINATION
        )

        logging.info("🏁 ETL Job Finished.")

    except Exception as e:
        logging.error(f"❌ ETL Job Failed: {e}")
        exception_caught = 1

    finally:
        # 4. Close spark session
        spark.stop()
        sys.exit(exception_caught)


if __name__ == "__main__":
    main(defaults, gcs)
