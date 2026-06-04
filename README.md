# Blent Spark Project

Data engineering pipeline using PySpark.

## GCP & Data Integration

To read data from Google Cloud Storage (GCS) instead of local files, follow these steps:

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

### 3. Accessing Data in GCS
Once authenticated and configured, you can use the `gs://` protocol in your Spark jobs. The setup process configures Application Default Credentials (ADC), so no JSON key file is required for local development.
