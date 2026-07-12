import os
import argparse
import configparser
import logging
import google.auth
from google.cloud import storage
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
from typing import List, Optional


@dataclass
class GCS:
    PROJECT_ID: str
    CREDENTIALS: google.auth.credentials.Credentials
    BUCKET_NAME: str
    bucket: storage.bucket
    BASE_DIR: str
    ABS_RAW_DIR: str
    REL_RAW_DIR: str
    storage: storage.Client
    API_URL: str
    filepaths: List[str]


def get_gcs_dict(config: configparser.ConfigParser) -> dict:
    # Goal: Initialize GCS object from dictionary

    gcs_dict = {
        "BUCKET_NAME": config.get("STORAGE", "BUCKET_NAME"),
        "REL_RAW_DIR": "",
        "ABS_RAW_DIR": "",
        "storage": storage.Client(),
        "API_URL": "https://storage.googleapis.com",
        "filepaths": []
    }
    # _, gcs_dict["PROJECT_ID"] = google.auth.default()
    # Get the numerical project ID from ADC
    gcs_dict["CREDENTIALS"], gcs_dict["PROJECT_ID"] = google.auth.default()
    gcs_dict["BASE_DIR"] = "gs://" + gcs_dict["BUCKET_NAME"]
    gcs_dict["bucket"] = gcs_dict["storage"].bucket(gcs_dict["BUCKET_NAME"])

    return gcs_dict

# _____________________________ Argument Parsing _____________________________
def parse_args(
    config: configparser.ConfigParser,
    gcs_base_dir: str,
    script_description: str
) -> argparse.Namespace:
    # Goal:
    # - get arguments from command line & config file
    # - set help description & list active parameters

    # Read default values from config file
    if "transfer" in script_description.lower():
        deft_raw_dir = config.get("TRANSFER", "REL_RAW_DIR")
    else:
        deft_raw_dir = config.get("SPARK_SCRIPT_DEFAULTS", "REL_RAW_DIR")
    deft_start_date = config.get("SPARK_SCRIPT_DEFAULTS", "DATE_START")
    deft_end_date = config.get("SPARK_SCRIPT_DEFAULTS", "DATE_END")
    DATE_FORMAT = config.get("GENERAL", "HELP_DATE_FORMAT")
    deft_out_dir = gcs_base_dir + "/" + \
        config.get("SPARK_SCRIPT_DEFAULTS", "REL_PROCESSED_DIR")

    # Function to correct path arguments
    def clean_path(path: str) -> str:
        """Remove trailing slash if present."""
        return path.rstrip(os.path.sep)

    # Create Parser
    parser = argparse.ArgumentParser(
        description=script_description
    )
    parser.add_argument(
        "--REL_RAW_DIR",
        type=clean_path,
        default=deft_raw_dir,
        help=f"GCS path to output dir (default: {deft_raw_dir})"
    )
    parser.add_argument(
        "--DESTINATION",
        type=clean_path,
        default=deft_out_dir,
        help=f"GCS path to output dir (default: {deft_out_dir})"
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


def search_file_in_bucket(gcs: GCS, filename: str) -> bool:
    # Goal: check if source raw file is available in GCS Bucket

    gcs_file_abs_path = f"{gcs.ABS_RAW_DIR}/{filename}"
    logging.info(f"⏭️ Looking in Bucket for: {gcs_file_abs_path}")
    gcs_file_rel_path = f"{gcs.REL_RAW_DIR}/{filename}"
    gcs_file_pointer = gcs.bucket.blob(gcs_file_rel_path)
    file_found_in_bucket = gcs_file_pointer.exists()

    # Prepare list of GCS filepaths to be processed by Spark job
    gcs.filepaths.append(gcs_file_abs_path)

    return file_found_in_bucket


def list_files_to_process(
        args: argparse.Namespace, 
        config: configparser.ConfigParser) -> List[str]:
    # Goal: Get list of files to process from the input date range

    # Convert date strings to datetime
    date_format = config.get("STORAGE", "DATE_FORMAT")

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

    # ToDo: delete this mock file
    # filenames = ["sample.csv"]

    return filenames


# _____________________________ Logging _________________

def setup_logging(config_vars: str, logfile_name: Optional[str]=None) -> None:
    """
    Setup logging from environment variables.
    Outputs to:
     - console
        - text in green
     - LOG_FILE
        - optional
        - in append mode
    """

    # Set Log Level
    log_level = config_vars.get("LOGGING", "LOG_LEVEL")
    level = getattr(
        logging,
        log_level,
        logging.INFO
    )

    # Set Log-to-Console Handler
    console_handler = logging.StreamHandler()

    # Set Log-to-Console Formatter
    color_green = "\033[92m"  # ANSI color code
    color_reset = "\033[0m"
    basic_format = config_vars.get("LOGGING", "LOG_FORMAT")
    custom_format = f"{color_green}{basic_format}{color_reset}"
    custom_formatter = logging.Formatter(
        fmt=custom_format
    )
    console_handler.setFormatter(custom_formatter)

    # Initialize list of handlers
    handlers = [console_handler]

    # Set Log-to-File Handler
    log_dir = config_vars.get("LOGGING", "LOG_DIR", fallback=None)
    if log_dir and logfile_name:
        # Create log directory if it doesn't exist
        Path(log_dir).mkdir(exist_ok=True)
        logfile_path = f"{log_dir}/{logfile_name}"
        logfile_handler = logging.FileHandler(
            Path(logfile_path),
            encoding="utf-8"
        )
        # Set Log-to-File Formatter
        basic_formatter = logging.Formatter(
            fmt=basic_format
        )
        logfile_handler.setFormatter(basic_formatter)

        # Update list of handlers
        handlers.append(logfile_handler)

    # Set Logging Configuration
    logging.basicConfig(
        level=level,
        handlers=handlers,
    )
