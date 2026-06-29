## ToDo:
- transfer 7 csv files from AWS to GCS
- run 
python -m src.etl_job --DATE_START="2019-12-15 00:00:00" --DATE_END="2020-01-15 00:00:00" --SOURCE="gs://blent_spark_bucket1/data/raw/sample.csv" --DESTINATION="gs://blent_spark_bucket1/data/processed"
- rename log file + git add it
- update README
- release + validate project
  
# Blent Spark Project

Data engineering pipeline using PySpark.

## Clone git repo
git clone https://github.com/JeanRosselVallee/Blent_Spark.git

## 1. Prototyping (Step 1 Validation)
The `src/nbook_prototype.ipynb` notebook validates the first step of the project: creating a Spark script on a data sample. It describes the step-by-step transformations required to obtain the final output table, using a sample of the data to ensure fast iterations and validation of the logic.

## 2. Local Unit Tests
- Goal: validate locally (before deploying to the Cloud) the ETL transformation functions of the Python script `src/etl_job.py`.
- Input: mock dataset `tests/mock_data.csv` 
- Expected results: `test/expected_output.csv`.
- A Pytest report compares actual vs. expected results that show:
  - a test's status (`PASSED` or `FAILED`) per feature
  - the record location and the mismatched values

**Run :**
```bash
# Activate the environment (if not already active)
source venv_spark/bin/activate

# Install dependencies
pip install -r requirements.txt

# Execute the test
pytest tests/test_etl_job.py -vv --disable-warnings
```

## 3. GCP & Data Integration (Step 2 Validation)

The Python-Spark script  `src/etl_job.py` validates the second step of the project:
- it reads the input table from a GCS source
- it accepts command-line arguments (`--SOURCE`, `--DESTINATION`, `--DATE_START`, `--DATE_END`)
- it saves the processed output table in CSV format to a GCS target destination.

### 3.1. Place Input Files in GCS
Create a Google Cloud Storage bucket named `blent_spark_bucket` and upload your raw data files under the `data/raw/` path inside the bucket.

### 3.2. Automated Setup & Authentication
Run the master setup script to initialize your virtual environment (`venv_spark`), install dependencies, and authenticate your GCP account. 

**Note:** We use `source` so that the environment variables (like the Google Cloud SDK paths) are applied to your current terminal session immediately:
```bash
chmod +x set_env.sh && source set_env.sh
```
*Note: This will open your browser to log in to your Google Account.*

### 3.3. Create a `.env` File
Create a `.env` file using the template to avoid hardcoding configuration values:
```bash
cp .env_template .env
```

### 3.4. Running the ETL Script (Job Parameterization)
The [etl_job.py](src/etl_job.py) script is designed to accept parameter configurations at runtime.

Activate the virtual environment first:
```bash
source venv_spark/bin/activate
```

#### Run with Defaults (from `.env`):
By default, the script reads `SOURCE` and `DESTINATION` from your `.env` configuration:
```bash
python -m src.etl_job
```

#### Run with Custom Parameter Override (Step 2 Requirement):
You can override the source data, target destination, and date boundaries when launching the Spark job:
```bash
python -m src.etl_job \
  --SOURCE "gs://blent_spark_bucket/data/raw/sample.csv" \
  --DESTINATION "gs://blent_spark_bucket/data/processed/features_output" \
  --DATE_START "2026-06-01 00:00:00" \
  --DATE_END "2026-06-15 23:59:59"
```

### 3.5. Output Format and Verification
Upon successful execution:
* The resulting output table will be written in **CSV format** directly to the specified `--DESTINATION` path in your object storage.
* Spark writes a `_SUCCESS` marker file in the destination folder once the write operation completes successfully.

---
**Note on Data Formats:**
While the CSV format is used here for interoperability and readability, in a high-throughput production environment, migrating to Parquet is highly recommended to optimize storage footprint on GCS/S3 and accelerate query speeds.
