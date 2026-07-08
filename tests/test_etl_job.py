"""
Syntax 1 :
    pytest tests/test_etl_job.py -vv --disable-warnings

Syntax 2 : less verbose
    pytest tests/test_etl_job.py -vv --disable-warnings --color=yes |
    awk '/FAILURES/ {hide=1};/summary/ {hide=0};{if (hide == 0) {print $0}}'
"""


import pandas as pd
import pytest
from pyspark.sql import functions as f
from old.old_etl_job import extract, transform


# A fixture is a 1-shot function
@pytest.fixture(scope="module")
# A session is opened by spark() (defined in ./conftest.py)
def get_actual_and_expected_pdfs(spark):

    BUCKET_FOLDER = "gs://blent_spark_bucket1/tests"
    INPUT_FILE_MOCK = f"{BUCKET_FOLDER}/mock_data.csv"
    OUTPUT_FILE_EXPECTED = f"{BUCKET_FOLDER}/expected_output.csv"
    # INPUT_FILE_MOCK = "./mock_data.csv"
    # OUTPUT_FILE_EXPECTED = "./expected_output.csv"

    # 1. Load input data using production code
    sdf_input = extract(spark, INPUT_FILE_MOCK)

    # 2. Call tested function
    sdf_actual = transform(sdf_input)
    pdf_actual = sdf_actual.toPandas() \
        .reset_index() \
        .set_index(["index", "user_session", "product_id"]) \
        .sort_index()

    # 3. Load expected output data
    sdf_expected = extract(spark, OUTPUT_FILE_EXPECTED)

    pdf_expected = sdf_expected.toPandas() \
        .reset_index() \
        .set_index(["index", "user_session", "product_id"]) \
        .sort_index()
    pdf_expected["start_time"] = pdf_expected["start_time"] \
        .dt.strftime("%H:%M")

    return pdf_actual, pdf_expected


# Fields to validate as an individual test cases
FIELDS_AS_TEST_CASES = [
    "category_code",
    "brand",
    "price",
    "purchased",
    "num_views_product",
    "user_id",
    "num_views_session",
    "start_time",
    "start_weekday",
    "duration",
    "num_prev_sessions",
    "num_prev_product_views"
]

# Generate array of test functions, 1 function per list item
@pytest.mark.parametrize("field_name", FIELDS_AS_TEST_CASES)
def test_field_equality(get_actual_and_expected_pdfs, field_name):
    """
    - gets the returned value of the 1-shot function
    - tests 1 field of actual pdf vs. expected pdf
    """
    pdf_actual, pdf_expected = get_actual_and_expected_pdfs

    # Get a pdf with index & a single field
    pdf_actual = pdf_actual[[field_name]]
    pdf_expected = pdf_expected[[field_name]]

    try:
        # Standard strict check
        pd.testing.assert_frame_equal(
            pdf_actual,
            pdf_expected,
            check_dtype=False
        )
    except AssertionError:
        # Print rows of pdfs in case of mismatch
        pdfs_comparison = pdf_actual.compare(
                pdf_expected,
                result_names=("actual", "expected")
            )
        error_msg = f"\n{pdfs_comparison.to_string()}\n"
        raise AssertionError(error_msg)
