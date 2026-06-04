"""
# Spark Transformation Prototype
This notebook demonstrates a basic PySpark ETL pipeline: 
- Extraction from Google Cloud Storage
- Transformation
- Export to GCS.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from gcsfs import GCSFileSystem


def load_config():
    """Loads configuration from environment variables."""
    load_dotenv()
    config = {
        "GC_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "GC_BUCKET": os.getenv("GCS_BUCKET_NAME"),
        "DATA_SUBDIR": os.getenv("DATA_SUBDIR"),
        "RAW_FILE_NAME": os.getenv("RAW_FILE_NAME"),
    }
    # Construct paths
    DATA_PATH = f"gs://{config['GC_BUCKET']}/{config['DATA_SUBDIR']}"
    config["RAW_FILE_PATH"] = f"{DATA_PATH}/raw/{config['RAW_FILE_NAME']}"
    config["OUTPUT_DIR"] = f"{DATA_PATH}/processed"
    return config


def create_spark_session(project_id):
    """
    Initializes a Spark Session with GCS support using ADC.
    Spark requires a connector to access the large files stored in GCS.
    """

    # Get credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    
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

    # 1. Reduce Spark's internal log level
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Directly silence the underlying Log4j logger (Java logs)
    log4jLogger = spark._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.ERROR)
    log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.ERROR)

    return spark


def extract(spark, input_path):
    """
    2. Ingestion: Read raw data
    """
    print(f"📥 Extracting data from: {input_path}...")
    df = spark.read.csv(
        input_path, 
        header=True, 
        inferSchema=True
    )
    print(f"✅ Extracted {df.count()} rows.")
    return df


def transform(df):
    """
    3. Transformation: Filter for 'view' events and select columns
    """
    print("⚙️ Transforming data...")
    transformed_df = df.filter(col("event_type") == "view") \
        .select("event_time", "brand", "price", "user_id") \
        .limit(1000) # Limit for prototype validation
    return transformed_df


def load(spark_df, output_path):
    """
    4. Export: Write to processed data folder and verify
    """
    print(f"📤 Loading data to: {output_path}...")
    spark_df \
        .write.mode("overwrite") \
        .csv(
            output_path, 
            header=True
        )
    
    # Verification using gcsfs
    success_file = f"{output_path}/_SUCCESS".replace("gs://", "")
    
    if GCSFileSystem().exists(success_file):
        print("✨ SUCCESS: Data Upload was successful and verified!")
    else:
        print("⚠️ WARNING: Data Upload may have failed. _SUCCESS file not found.")


def main():
    print("🚀 Starting ETL Job...")
    # 1. Setup
    config = load_config()
    spark = create_spark_session(config["GC_PROJECT_ID"])
    
    try:
        # 2. Extract
        raw_data = extract(spark, config["RAW_FILE_PATH"])
        
        # 3. Transform
        processed_data = transform(raw_data)
        
        # 4. Load
        load(processed_data, config["OUTPUT_DIR"])
        
    finally:
        # Close spark session
        spark.stop()
        print("🏁 ETL Job Finished.")

if __name__ == "__main__":
    main()
