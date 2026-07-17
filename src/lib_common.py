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
from typing import List, Tuple, Optional


# ____________________ Variables Initialization _____________________


@dataclass
class GCS:
    PROJECT_ID: str
    CREDENTIALS: google.auth.credentials.Credentials
    BUCKET_NAME: str
    bucket: storage.bucket
    BASE_DIR: str
    REL_RAW_DIR: str
    REL_RAW_SUBDIR: str
    ABS_RAW_DIR: str
    storage: storage.Client
    API_URL: str
    ARG_DATE_FORMAT: str
    SOURCE_URL_DIR: str
    MAX_NB_CACHED_ROWS: int
    filepaths: List[str]
    DEBUG_ENABLED: bool


def get_gcs_dict(config_ini: configparser.ConfigParser) -> dict:
    # Goal: Initialize GCS object from dictionary
    
    logging.info("Initializing GCS object from dictionary...")
    gcs_dict = {
        "BUCKET_NAME": config_ini.get("STORAGE", "BUCKET_NAME"),
        "REL_RAW_DIR": config_ini.get("STORAGE", "REL_RAW_DIR"),
        "ABS_RAW_DIR": "",
        "storage": storage.Client(),
        "API_URL": "https://storage.googleapis.com",
        "ARG_DATE_FORMAT": config_ini.get("STORAGE", "DATE_FORMAT"),
        "SOURCE_URL_DIR": config_ini.get("STORAGE", "SOURCE_URL_DIR"),
        "MAX_NB_CACHED_ROWS": int(config_ini.get("SPARK", "MAX_NB_CACHED_ROWS")),
        "filepaths": [],
        "DEBUG_ENABLED": config_ini.get("GENERAL", "DEBUG_ENABLED")
    }

    gcs_dict["CREDENTIALS"], gcs_dict["PROJECT_ID"] = google.auth.default()
    gcs_dict["BASE_DIR"] = "gs://" + gcs_dict["BUCKET_NAME"]
    gcs_dict["bucket"] = gcs_dict["storage"].bucket(gcs_dict["BUCKET_NAME"])
    gcs_dict["REL_RAW_SUBDIR"] = gcs_dict["SOURCE_URL_DIR"].lstrip("https://")

    return gcs_dict


def parse_args(
    config_ini: configparser.ConfigParser,
    gcs: GCS,
    script_description: str
) -> argparse.Namespace:
    # Goal:
    # - get arguments from command line & config_ini file
    # - set help description & list active parameters

    logging.info("Parsing arguments...")

    # Read default values from config_ini file
    deft_raw_dir = gcs.REL_RAW_DIR
    deft_start_date = config_ini.get("SPARK", "DATE_START")
    deft_end_date = config_ini.get("SPARK", "DATE_END")
    DATE_FORMAT = config_ini.get("GENERAL", "HELP_DATE_FORMAT")
    deft_out_dir = gcs.BASE_DIR + "/" + \
        config_ini.get("SPARK", "REL_PROCESSED_DIR")

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
    if "spark" in script_description.lower():
        deft_raw_dir = f"{deft_raw_dir}/{gcs.REL_RAW_SUBDIR}"
    [logging.info(f"  • {key}: {val}") for key, val in args_dict.items()]

    return args


def apply_config_values(
        config_path: str,
        script_description: str,
        logfile_name: Optional[str] = None
) -> Tuple[GCS, argparse.Namespace]:
    # Apply config_ini variables to Dataclass "gcs" and to parsed arguments
    print("Applying config_ini variables...")

    # Read configuration file
    config_ini = configparser.ConfigParser()
    config_ini.read(config_path)

    # Configure Logging
    setup_logging(config_ini, script_description, logfile_name)

    # Create an intermediate variable for instanciation
    gcs_dict = get_gcs_dict(config_ini)

    # Instanciation
    gcs = GCS(**gcs_dict)

    # Parse Arguments
    args = parse_args(config_ini, gcs, script_description)

    # Update GCS with parsed args
    gcs.REL_RAW_DIR = args.REL_RAW_DIR
    gcs.ABS_RAW_DIR = f"{gcs.BASE_DIR}/{gcs.REL_RAW_DIR}"

    return gcs, args

# _____________________________ Raw Files _________________


def search_file_in_bucket(gcs: GCS, filename: str) -> bool:
    # Goal: check if source raw file is available in GCS Bucket

    gcs_file_rel_path = f"{gcs.REL_RAW_DIR}/{gcs.REL_RAW_SUBDIR}/{filename}"
    gcs_file_abs_path = f"{gcs.BASE_DIR}/{gcs_file_rel_path}"
    logging.info(f"⏭️ Looking in Bucket for: {gcs_file_abs_path}")
    gcs_file_pointer = gcs.bucket.blob(gcs_file_rel_path)
    file_found_in_bucket = gcs_file_pointer.exists()

    # Prepare list of GCS filepaths to be processed by Spark job
    gcs.filepaths.append(gcs_file_abs_path)

    return file_found_in_bucket


def list_files_to_process(
        args: argparse.Namespace,
        gcs: GCS) -> List[str]:
    # Goal: Get list of files to process from the input date range

    # Convert date strings to datetime
    date_format = gcs.ARG_DATE_FORMAT

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

    if gcs.DEBUG_ENABLED:  # In Debug Mode, process a sample file
        filenames = ["sample.csv"]

    return filenames


# _____________________________ Logging _________________

def setup_logging(
        config_vars: configparser.ConfigParser,
        script_description: str,
        logfile_name: Optional[str] = None
) -> None:
    """
    Setup logging from environment variables.
    Outputs to:
     - console
        - text in green
     - LOG_FILE
        - optional
        - in append mode
    """

    print(f"Setting up Logging for {script_description}")

    # Set Log Level
    level = config_vars.get("LOGGING", "LOG_LEVEL", fallback="INFO")

    # Set Log-to-Console Handler
    console_handler = logging.StreamHandler()

    # Set Log-to-Console Formatter
    basic_format = config_vars.get("LOGGING", "LOG_FORMAT")
    if "spark" in script_description.lower():
        # Case of Spark Job
        custom_formatter = logging.Formatter(
            fmt=basic_format
        )
    else:
        # Case of Transfer Job
        color_green = "\033[92m"  # ANSI color code
        color_reset = "\033[0m"
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
