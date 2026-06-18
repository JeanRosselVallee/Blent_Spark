import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Fixture to create a local SparkSession for testing."""

    session = SparkSession.builder \
        .appName("PySpark-Unit-Tests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    yield session
    session.stop()
