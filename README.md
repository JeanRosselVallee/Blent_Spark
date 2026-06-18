# Blent Spark Project

Data engineering pipeline using PySpark.

## Clone git repo
git clone https://github.com/JeanRosselVallee/Blent_Spark.git

## 1. Prototyping (Step 1 Validation)
The `src/jobs/nbook_prototype.ipynb` notebook validates the first step of the project: creating a Spark script on a data sample. It describes the step-by-step transformations required to obtain the final output table, using a sample of the data to ensure fast iterations and validation of the logic.

## 2. GCP & Data Integration

To read data from Google Cloud Storage (GCS) instead of local files, follow these steps:

### Place input files in a GCS Bucket
Create Bucket "blent_spark_bucket"
import input file under data/raw/

### 2.1. Automated Setup & Authentication
Run the master setup script to initialize your environment (venv_spark), install dependencies, and authenticate your GCP account. 

**Note:** We use `source` so that the environment variables (like the GCloud PATH) are applied to your current terminal session immediately:
```bash
chmod +x set_env.sh && source set_env.sh
```
*Note: This will open your browser twice to log in to your Google Account.*

### 2.2. Create a `.env` File
Create an `.env` with the contents of .env.template. This file is used by the notebook and scripts to avoid hardcoding. It contains GCP & GCS configuration.
```bash
cp .env.template .env
```

### 2.3. Running the ETL Script
After setup and configuration, you can run the production-ready ETL script:
```bash
# Activate the environment
source venv_spark/bin/activate

# Run the script
python src/jobs/etl_job.py
```

### 2.4. Accessing Data in GCS
The script uses the `gs://` protocol and configures Application Default Credentials (ADC), so no JSON key file is required for local development. It creates a `_SUCCESS` file in the output folder upon completion.

---
**Note sur le format des données :**
Bien que le format CSV ait été choisi ici pour des raisons d'interopérabilité et de facilité de lecture de l'échantillon, dans un environnement de production à grande échelle, un passage au format Parquet serait fortement recommandé pour optimiser les coûts de stockage sur GCS et accélérer les requêtes futures.
