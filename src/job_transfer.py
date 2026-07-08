"""
Goal: Transfer source files from AWS S3 to GCS Bucket via STS
- Check if source files are already in Bucket
- If not, create a TSV file with the list of pending URLs
- Entirely performed in the Master Node
Usage:
python3 -m src.transfer_job

gcloud dataproc jobs submit pyspark \
    gs://blent_spark_bucket4/src/transfer_raw_files.py \
    --cluster=main-cluster --region=us-central1 \
    --files=gs://blent_spark_bucket4/src/config.ini
"""

# _____________________________ Modules _____________________________

import logging
import argparse
import configparser
import sys
import time  # implements sleep()
import google.auth  # gets default project ID from GOOGLE_CLOUD_PROJECT
import requests  # check if source hyperlink is valid
from dataclasses import dataclass
from google.cloud import storage
from typing import List, Tuple
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

# # Install additional module (absent in GCP Master Node by default)
# extra_module = "google-cloud-storage-transfer==" + \
#     "{config.get('STORAGE', 'STORAGE_TRANSFER_VERSION')}"
# subprocess.check_call([
#     sys.executable,  # executes Python
#     "-m", "pip", "install", "--quiet",
#     "google-cloud-storage-transfer==1.19.0",
#     "--no-deps"  # leaves core GCS modules untouched
# ])

from google.cloud import storage_transfer_v1  # storage transfer to GCS


# ____________________ Variables Initialization _____________________

# Read configuration file
config = configparser.ConfigParser()
config.read("src/config.ini")  # Debug local execution
# config.read("config.ini")  # Production remote execution

# Configure Logging
logging.basicConfig(
    level=config.get("LOGGING", "level"),
    format=config.get("LOGGING", "format")
)

# Set GCS values


@dataclass
class GCS:
    PROJECT_ID: str
    CREDENTIALS: google.auth.credentials.Credentials
    BUCKET_NAME: str
    bucket: storage.bucket
    GCS_ABS_RAW_DIR: str
    GCS_REL_RAW_DIR: str
    storage: storage.Client
    API_URL: str
    file_paths: List[str]


gcs_dict = {
    "BUCKET_NAME": config.get("STORAGE", "BUCKET_NAME"),
    "GCS_REL_RAW_DIR": "",
    "GCS_ABS_RAW_DIR": "",
    "storage": storage.Client(),
    "API_URL": "https://storage.googleapis.com",
    "file_paths": []
}
# _, gcs_dict["PROJECT_ID"] = google.auth.default()
# Get the numerical project ID from ADC
gcs_dict["CREDENTIALS"], gcs_dict["PROJECT_ID"] = google.auth.default()
GCS_BASE_DIR = "gs://" + gcs_dict["BUCKET_NAME"]
gcs_dict["bucket"] = gcs_dict["storage"].bucket(gcs_dict["BUCKET_NAME"])

gcs = GCS(**gcs_dict)


def parse_args(config: configparser.ConfigParser) -> argparse.Namespace:
    # Goals:
    # - get arguments from command line & config file
    # - set help description & list active parameters

    # Read default values from config file
    deft_raw_dir = config.get("SPARK_SCRIPT_DEFAULTS", "GCS_REL_RAW_DIR")
    deft_start_date = config.get("SPARK_SCRIPT_DEFAULTS", "DATE_START")
    deft_end_date = config.get("SPARK_SCRIPT_DEFAULTS", "DATE_END")
    DATE_FORMAT = config.get("GENERAL", "HELP_DATE_FORMAT")
    
    parser = argparse.ArgumentParser(
        description="Cloud-Transfer Raw Files"
    )
    parser.add_argument(
        "--GCS_REL_RAW_DIR",
        type=str,
        default=deft_raw_dir,
        help=f"GCS path to output dir (default: {deft_raw_dir})"
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
    args = parser.parse_args()

    # Summary List of Active Parameters
    logging.info("🔧 Active Parameters:")
    args_dict = vars(args)
    [logging.info(f"  • {key}: {val}") for key, val in args_dict.items()]

    return args


def list_files_to_process(args: argparse.Namespace) -> List[str]:
    # Goal: Get list of files to process from the input date range

    # Convert date strings to datetime
    date_format = config.get("STORAGE", "date_format")

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


def list_files_to_transfer(gcs: GCS, files_to_process: List[str]) -> List[str]:
    # Goal: get files "pending" or missing in the bucket

    files_to_transfer = []
    for filename in files_to_process:
        # Check source is available in Bucket
        gcs_file_abs_path = f"{gcs.GCS_ABS_RAW_DIR}/{filename}"
        logging.info(f"⏭️ Looking in Bucket for: {gcs_file_abs_path}")
        gcs.file_paths.append(gcs_file_abs_path)
        gcs_file_rel_path = f"{gcs.GCS_REL_RAW_DIR}/{filename}"
        gcs_file_pointer = gcs.bucket.blob(gcs_file_rel_path)
        if gcs_file_pointer.exists():
            logging.info(f"⏭️ File {filename} is already in Bucket")
            continue
        else:
            logging.info(f"📝 File {filename} is pending")

            # ToDo: delete this mock file
            filename = "sample.csv"

            # Check source is available in S3
            url_base_path = config.get("STORAGE", "AWS_URL_DIR")
            url_file = f"{url_base_path}/{filename}"
            logging.info(f"⏭️ Checking validity of link: {url_file}")
            response = requests.head(
                url_file,
                allow_redirects=True,  # if URL points to another
                timeout=5  # in seconds, default: waits response forever
            )
            if response.status_code != 200:
                raise FileNotFoundError(f"❌ URL {url_file} not valid")

            file_size = response.headers.get("Content-Length", "0")
            files_to_transfer.append(f"{url_file}\t{file_size}")

    return files_to_transfer


def create_tsv_file(gcs: GCS, tsv_contents: List[str]) -> str:
    # Goal 1: List pending URLs in TSV file (as required by STS)
    # Goal 2: Upload TSV file to GCS Bucket & return its URL

    tsv_content = "TsvHttpData-1.0\n" + "\n".join(tsv_contents) + "\n"
    tsv_rel_file_path = "tsv/aws_pending_urls.tsv"
    tsv_pointer = gcs.bucket.blob(tsv_rel_file_path)
    tsv_pointer.upload_from_string(
        tsv_content,
        content_type="text/tab-separated-values",
        predefined_acl="publicRead"
    )

    # Get URL to TSV file in GCS Bucket
    tsv_url = f"{gcs.API_URL}/{gcs.BUCKET_NAME}/{tsv_rel_file_path}"
    logging.info(f"Created list of URLs to download at: {tsv_url}")

    return tsv_url


def define_transfer_job(gcs: GCS, tsv_url: str) -> Tuple[str, dict]:
    """
    Goal 1: Define the Transfer Job to be submitted to STS
    Goal 2: Automatically create Google-managed STS account (Master node)
    
    """
    # Set Job's unique timestamped name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    compulsory_prefix = "transferJobs/"
    job_name = f"{compulsory_prefix}blent_{timestamp}"

    # Define Transfer Job
    logging.info(f"Defining Transfer Job: {job_name} ...")
    job_definition = {
        "name": job_name,
        "project_id": gcs.PROJECT_ID,
        #"status": storage_transfer_v1.TransferJob.Status.ENABLED,
        "status": "ENABLED",  # so job waits for permissions
        "description": "Cloud-Transfer of Raw Files",
        "transfer_spec": {
            "http_data_source": {
                "list_url": tsv_url,  # ************* Source
            },
            "gcs_data_sink": {
                "bucket_name": gcs.BUCKET_NAME,  # Destination
                "path": gcs.GCS_REL_RAW_DIR + "/"
            }
        }
    }

    return job_name, job_definition


def run_transfer_job(gcs: GCS, job_name: str, job_definition: dict) -> dict:
    """
    Roles to be granted to the accounts involved in the transfer job:
    |--------------------|-------|-------|-------------|-------------|
    |    Role to grant   |Account|Manager|   Used by   |transfer task|
    |--------------------|-------|-------|-------------|-------------|
    |STS User            | User  |Blent  |Operator User|Call APIs    |
    |STS User            |Service|User   |Transf-Master|Manage job   |
    |Bucket Storage Admin|Service|Google |Transf-Worker|Perform job  |
    |--------------------|-------|-------|-------------|-------------|
    - Add "Service Account User" role if Master needs to impersonate workers.
    - The 2 Service Accounts are created when the service is first used.
    """

    print("✅ Grant Transfer permissions to Master client via credentials")
    master_client = storage_transfer_v1.StorageTransferServiceClient(
        credentials=gcs.CREDENTIALS
    )

    # # Load credentials for the Master
    # print("✅ Load credentials for the Master")
    # # ToDo: Update these comments
    # # 1. Explicitly load the credentials file at runtime
    # SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
    # client_credentials = service_account.Credentials \
    #     .from_service_account_file(
    #         'credentials.json',
    #         scopes=SCOPES
    #     )
    # from google.auth.transport.requests import Request
    # # 🔑 Refresh the credentials to get a valid token
    # client_credentials.refresh(Request())
    
    # # Create the Workers SA
    # print("✅ Workers Transfer SA creation")
    # access_token = client_credentials.token
    # response = requests.get(
    #     f"https://storagetransfer.googleapis.com/v1/googleServiceAccounts/{gcs.PROJECT_ID}",
    #     headers={"Authorization": f"Bearer {access_token}"}
    # )
    # if response.status_code == 200:
    #     print("✅ STS Agent exists!")
    #     print(response.json())  # Shows the agent email
    #     data = response.json()
    #     workers_service_email = data['accountEmail']
    # else:
    #     raise ValueError(f"❌ Failed: {response.status_code} - {response.text}")

    # # Grant permissions on bucket to Transfer Workers' account    
    # logging.info("✅ Granting permissions on bucket to Workers' account")

    # # Update IAM policy
    # iam_policy = gcs.bucket.get_iam_policy(requested_policy_version=3)
    # iam_policy.bindings.append({  # Add role to Workers' account
    #     "role": "roles/storage.admin",
    #     "members": {f"serviceAccount:{workers_service_email}"}
    # })
    # logging.info("Point L")
    # gcs.bucket.set_iam_policy(iam_policy)

    logging.info("Create Transfer Job ")
    # Create Transfer Job (automatically creates workers' STS Account)
    master_client.create_transfer_job({
        "transfer_job": job_definition
    })

    # Run Transfer Job
    logging.info(f"🚀 Started File Transfer Job: {job_name}")
    master_client \
        .run_transfer_job({
            "job_name": job_name,
            "project_id": gcs.PROJECT_ID
        })

    return master_client, job_name


def monitor_transfer(gcs, master_client, job_name):
    # Monitor progress of transfer operations

    from google.protobuf.json_format import MessageToDict

    logging.info("⏳ Monitoring cloud transfer... (0% Spark hardware usage)")
    operations_client = master_client.transport.operations_client

    while True:

        try:  # inner try: to catch up at next operation
            # Update job status
            transfer_job_updated = master_client \
                .get_transfer_job({
                    "job_name": job_name,
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



"""
ToDo : 
Add def monitoring()
clean code
title authorisation sections
map authorisations, credentials, accounts, roles, scope, etc.
"""

# ==========================================================


def main(config: configparser.ConfigParser) -> str:
    try:
        step = "1. Parse Arguments"
        args = parse_args(config)
        gcs.GCS_REL_RAW_DIR = args.GCS_REL_RAW_DIR
        gcs.GCS_ABS_RAW_DIR = f"{GCS_BASE_DIR}/{gcs.GCS_REL_RAW_DIR}"

        step = "2. Get Input Filenames"
        files_to_process = list_files_to_process(args)

        step = "3. Get Pending Files"
        files_to_transfer = list_files_to_transfer(gcs, files_to_process)

        if len(files_to_transfer) > 0:

            step = "4. Create TSV File"
            tsv_url = create_tsv_file(gcs, files_to_transfer)

            step = "5. Define Transfer Job"
            job_name, job_definition = define_transfer_job(gcs, tsv_url)

            step = "6. Run Transfer Job"
            master_client, job_name = run_transfer_job(gcs, job_name, job_definition)

            step = "7. Monitor progress of Transfer Job"
            monitor_transfer(gcs, master_client, job_name)

        return gcs.file_paths

    except Exception as e:
        logging.error(f"❌ Error in step {step}:\n{e}")
        sys.exit(1)


if __name__ == "__main__":
    main(config)
