"""
Goal: Cloud-Transfer source raw files from AWS S3 to GCS Bucket via STS

Key Features:
- Validates file availability in both S3 and GCS
- Creates TSV files for STS transfer jobs
- Monitors transfer progress in real-time
- Launched locally but performed remotely in GCP

Usage:
python3 -m src.job_transfer
"""

# _____________________________ Modules _____________________________

import logging
import configparser
import sys
import time  # implements sleep()
import requests  # check if source hyperlink is valid
from typing import List, Tuple
from datetime import datetime, timezone
from google.cloud import storage_transfer_v1  # storage transfer to GCS
from .lib_common import GCS, get_gcs_dict, setup_logging, parse_args, \
                        list_files_to_process, search_file_in_bucket


# ____________________ Variables Initialization _____________________

# Read configuration file
config = configparser.ConfigParser()
config.read("src/config.ini")  # Debug local execution


def get_source_urls(gcs: GCS, files_to_process: List[str]) \
    -> Tuple[List[str], List[str]]:
    # Goal: get files "pending" or missing in the bucket

    source_urls = []
    source_filenames = []
    for filename in files_to_process:
        # Check source is available in Bucket
        file_found_in_bucket = search_file_in_bucket(gcs, filename)
                
        # if gcs_file_pointer.exists():
        if file_found_in_bucket:
            logging.info(f"⏭️ File {filename} is already in Bucket")
            continue
        else:
            logging.info(f"📝 File {filename} is pending")

            # ToDo: delete this mock file
            # filename = "sample.csv"

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
            source_urls.append(f"{url_file}\t{file_size}")
            source_filenames.append(filename)

    return source_urls, source_filenames


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
    logging.info(f"⏭️ Defining Transfer Job: {job_name} ...")
    job_definition = {
        "name": job_name,
        "project_id": gcs.PROJECT_ID,
        "status": "ENABLED",  # so job waits for permissions
        "description": "Cloud-Transfer of Raw Files",
        "transfer_spec": {
            "http_data_source": {
                "list_url": tsv_url,  # ************* Source
            },
            "gcs_data_sink": {
                "bucket_name": gcs.BUCKET_NAME,  # Destination
                "path": gcs.REL_RAW_DIR + "/"
            }
        }
    }

    return job_name, job_definition


def run_transfer_job(gcs: GCS, job_name: str, job_definition: dict) -> Tuple[dict, str]:
    """
    Required Roles for accounts involved in Transfer job:
    |---------------------------|-------|-------|---------------|-------------|
    |           Roles           |Account|Manager|    Used by    |transfer task|
    |---------------------------|-------|-------|---------------|-------------|
    |Storage Admin & objectAdmin| User  |Blent  |Operator User  |Call APIs    |
    |Storage Transfer User      |Service|User   |Transfer-Master|Manage job   |
    |Storage Admin              |Service|Google |Transfer-Worker|Perform job  |
    |---------------------------|-------|-------|---------------|-------------|
    Cf. setup_data_services.sh where these roles were granted. 
    """

    logging.info("✅ Grant Transfer permissions to Master client via credentials")
    master_client = storage_transfer_v1.StorageTransferServiceClient(
        credentials=gcs.CREDENTIALS
    )

    logging.info("⏭️ Create Transfer Job ")
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

    logging.info("⏭️ Monitoring cloud transfer...")
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
                logging.info("Initializing transfer...")
            else:
                current_operation = operations_client \
                    .get_operation(current_operation_name)

                # Get counters from metadata if available
                if current_operation.metadata:
                    metadata_dict = MessageToDict(current_operation.metadata)
                    counters = metadata_dict.get("counters", {})

                if not current_operation.done:
                    # Phase 2. Execution
                    
                    # Show progression percentage if available
                    if current_operation.metadata:
                        s_bytes_loaded = counters.get("bytesCopiedToSink", 0)
                        s_bytes_total = counters.get("bytesFoundFromSource", 0)
                        counters_are_valid = (s_bytes_loaded and s_bytes_total)
                        if not counters_are_valid:
                            logging.info("Transfer in progress: ... ⏳")
                        else:
                            ratio = int(s_bytes_loaded) / int(s_bytes_total)
                            total_MB = int(s_bytes_total) / 1024 / 1024
                            s_stats = f"{ratio*100:.1f}% of {total_MB:.1f} MB"
                            logging.info(f"Transfer in progress: {s_stats} ⏳")
                            
                else:
                    # Phase 3. Conclusion
                    logging.info("✅ Transfer completed!")
                    # Parse the operation metadata
                    if current_operation.metadata:
                        # metadata_dict = MessageToDict(current_operation.metadata)
                        # counters = metadata_dict.get("counters", {})
                        logging.info(f"📊 Transfer Summary:\n{counters}")
                    break

            time.sleep(15)  # Check every 15 seconds
        except Exception as e:
            raise ValueError(f"❌ Transfer Failed: {e}")


def main(config: configparser.ConfigParser) -> str:
    try:
        setup_logging(config, "job_transfer.log")
        logging.info("🚀 Starting Transfer Job")
        
        step = "1. Parse Arguments"
        gcs_dict = get_gcs_dict(config)
        gcs = GCS(**gcs_dict)
        args = parse_args(config, gcs.BASE_DIR, "Transfer Job")
        gcs.REL_RAW_DIR = args.REL_RAW_DIR
        gcs.ABS_RAW_DIR = f"{gcs.BASE_DIR}/{gcs.REL_RAW_DIR}"

        step = "2. Get Input Filenames"
        files_to_process = list_files_to_process(args, config)

        step = "3. Get Pending Files"
        source_urls, source_filenames = get_source_urls(
            gcs,
            files_to_process
        )

        if len(source_urls) > 0:

            step = "4. Create TSV File"
            tsv_url = create_tsv_file(
                gcs,
                source_urls
            )

            step = "5. Define Transfer Job"
            job_name, job_definition = define_transfer_job(
                gcs,
                tsv_url
            )

            step = "6. Run Transfer Job"
            master_client, job_name = run_transfer_job(
                gcs,
                job_name,
                job_definition
            )

            step = "7. Monitor progress of Transfer Job"
            logging.info(f"List of files to transfer: {source_filenames}")
            monitor_transfer(
                gcs,
                master_client,
                job_name
            )

        logging.info("🏁 Transfer Job Finished.")
        logging.info(f"⏭️ Available files in GCS storage:\n{gcs.filepaths}")
        return

    except Exception as e:
        logging.error(f"❌ Error in step {step}:\n{e}")
        sys.exit(1)


if __name__ == "__main__":
    main(config)
