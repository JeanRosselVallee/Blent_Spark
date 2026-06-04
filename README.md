# Blent Spark Project

Data engineering pipeline using PySpark.

## Clone git repo
git clone https://github.com/JeanRosselVallee/Blent_Spark.git


## GCP & Data Integration

To read data from Google Cloud Storage (GCS) instead of local files, follow these steps:

### Place input files in a GCS Bucket
Create Bucket "blent_spark_bucket"
import input file under data/raw/

### 1. Automated Setup & Authentication
Run the master setup script to initialize your environment (venv_spark), install dependencies, and authenticate your GCP account:
```bash
chmod +x setup_env_gcs.sh && ./setup_env_gcs.sh
```
*Note: This will open your browser twice to log in to your Google Account.*

### 2. Configuration (.env)
Create a `.env` file in the project root to store your GCP and GCS details. This file is used by the notebook and scripts to avoid hardcoding:
```bash
# GCP Configuration
GCP_PROJECT_ID=blent-sandbox-8950789090

# GCS Storage Configuration
GCS_BUCKET_NAME=dataproc-staging-us-central1-134288453953-pahddvko
GCS_RAW_DATA_PATH=data/raw/sample.csv
```

### 3. Running the ETL Script
After setup and configuration, you can run the production-ready ETL script:
```bash
# Activate the environment
source venv_spark/bin/activate

# Run the script
python src/jobs/etl_gcs.py
```

### 4. Accessing Data in GCS
The script uses the `gs://` protocol and configures Application Default Credentials (ADC), so no JSON key file is required for local development. It creates a `_SUCCESS` file in the output folder upon completion.
