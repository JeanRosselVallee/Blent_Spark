import pytest
from pyspark.sql import SparkSession
from old.old_etl_job import create_spark_session


@pytest.fixture(scope="session")
def spark():
    """Fixture to create a local SparkSession for testing."""

    session = SparkSession.builder \
        .appName("PySpark-Unit-Tests") \
        .master("local[*]") \
        .config(  # generate a single file instead of many part-files
            "spark.sql.shuffle.partitions",
            "1"  # default=200
        ) \
        .config(
            "spark.hadoop.fs.gs.user.project.lookup.mode",
            "NONE"  # to avoid Error on GCS billing quota for a project
        ) \
        .config(  # AWS & GCS Storage ===============================
            "spark.jars.packages",  # Spark syntax for list = "...,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.8,"
            "org.apache.hadoop:hadoop-aws:3.4.2"
        ) \
        .config(  # GCS Public Bucket ===============================
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        ) \
        .config(
            "spark.hadoop.fs.gs.impl.disable.cache",
            "true"
        ) \
        .config(
            "spark.hadoop.fs.gs.auth.type",
            "UNAUTHENTICATED"
        ) \
        .config(
            "spark.hadoop.fs.gs.project.id",
            "blent-spark-project"
        ) \
        .config(  # AWS S3 ==========================================
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        ) \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        ) \
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.eu-west-3.amazonaws.com"
        ) \
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "eu-west-3"
        ) \
        .config(  # General =========================================
            "spark.ui.showConsoleProgress",
            "false"
        ) \
        .getOrCreate()

    # If GCS authentication can't be set up, try from terminal:
    # export GCS_AUTH_TYPE=UNAUTHENTICATED
    # export GOOGLE_APPLICATION_CREDENTIALS=""

    # Test functions run on this session while fixture function waits
    # Replaces a less efficient "return"
    yield session
    session.stop()
