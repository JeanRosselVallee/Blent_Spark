"""
Goal: Shared utilities for GCS-based ETL pipelines (Transfer + Spark jobs).

Key features:
- Configuration parsing from INI files and CLI args (argparse)
- Dataclass (CONF_VARS) holding GCS client, bucket, paths & runtime flags
- GCS file discovery: existence check and month-range filename generation
- Centralized logging setup with console (color-coded) + optional file output
- Credentials via google.auth.default() for Transfer job
"""

import os
import sys
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
class CONF_VARS:
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


def get_dict_conf(config_ini: configparser.ConfigParser) -> dict:
    # Goal: Initialize CONF_VARS object from dictionary

    logging.info("Initializing CONF_VARS object from dictionary...")
    dict_conf = {
        "BUCKET_NAME": config_ini.get("STORAGE", "BUCKET_NAME"),
        "REL_RAW_DIR": config_ini.get("STORAGE", "REL_RAW_DIR"),
        "ABS_RAW_DIR": "",
        "storage": storage.Client(),
        "API_URL": "https://storage.googleapis.com",
        "ARG_DATE_FORMAT": config_ini.get("STORAGE", "DATE_FORMAT"),
        "SOURCE_URL_DIR": config_ini.get("STORAGE", "SOURCE_URL_DIR"),
        "MAX_NB_CACHED_ROWS": config_ini.getint("SPARK", "MAX_NB_CACHED_ROWS"),
        "filepaths": [],
        "DEBUG_ENABLED": config_ini.getboolean("GENERAL", "DEBUG_ENABLED")
    }

    dict_conf["CREDENTIALS"], dict_conf["PROJECT_ID"] = google.auth.default()
    dict_conf["BASE_DIR"] = f"gs://{dict_conf['BUCKET_NAME']}"
    dict_conf["bucket"] = dict_conf["storage"].bucket(dict_conf["BUCKET_NAME"])
    dict_conf["REL_RAW_SUBDIR"] = dict_conf["SOURCE_URL_DIR"] \
        .lstrip("https://")

    return dict_conf


def parse_arguments(
    config_ini: configparser.ConfigParser,
    conf_vars: CONF_VARS,
    script_description: str
) -> argparse.Namespace:
    # Goal:
    # - get arguments from command line & config_ini file
    # - set help description & list active parameters

    logging.info("Parsing arguments...")

    # Read default values from config_ini file
    deft_raw_dir = conf_vars.REL_RAW_DIR
    deft_start_date = config_ini.get("SPARK", "DATE_START")
    deft_end_date = config_ini.get("SPARK", "DATE_END")
    DATE_FORMAT = config_ini.get("GENERAL", "HELP_DATE_FORMAT")
    deft_out_dir = conf_vars.BASE_DIR + "/" + \
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
    arguments = parser.parse_args()

    # Summary List of Active Parameters
    logging.info("🔧 Active Parameters:")
    arguments_dict = vars(arguments)
    if "spark" in script_description.lower():
        deft_raw_dir = f"{deft_raw_dir}/{conf_vars.REL_RAW_SUBDIR}"
    [logging.info(f"  • {key}: {val}") for key, val in arguments_dict.items()]

    return arguments


def apply_config_values(
        config_path: str,
        script_description: str,
        logfile_name: Optional[str] = None
) -> Tuple[CONF_VARS, argparse.Namespace]:
    # Apply variables from config_ini to Dataclass & to parsed arguments
    print("Applying variables from config_ini...")

    # Read configuration file
    config_ini = configparser.ConfigParser()
    config_ini.read(config_path)

    # Configure Logging
    setup_logging(config_ini, script_description, logfile_name)

    # Create an intermediate variable for instanciation
    dict_conf = get_dict_conf(config_ini)

    # Instanciation
    conf_vars = CONF_VARS(**dict_conf)

    # Parse Arguments
    arguments = parse_arguments(config_ini, conf_vars, script_description)

    # Update CONF_VARS with parsed arguments
    conf_vars.REL_RAW_DIR = arguments.REL_RAW_DIR
    conf_vars.ABS_RAW_DIR = f"{conf_vars.BASE_DIR}/{conf_vars.REL_RAW_DIR}"

    # Check parsed argument DESTINATION is a subdir of Bucket
    bucket_from_path = arguments.DESTINATION.split("/")[2]
    if not (bucket_from_path == conf_vars.BUCKET_NAME):
        logging.error("❌ Argument DESTINATION is not a subdir of Bucket.")
        sys.exit(1)

    return conf_vars, arguments

# _____________________________ Raw Files _________________


def search_file_in_bucket(conf_vars: CONF_VARS, filename: str) -> bool:
    # Goal: check if source raw file is available in GCS Bucket

    gcs_file_rel_path = \
        f"{conf_vars.REL_RAW_DIR}/{conf_vars.REL_RAW_SUBDIR}/{filename}"
    gcs_file_abs_path = f"{conf_vars.BASE_DIR}/{gcs_file_rel_path}"
    logging.info(f"⏭️ Looking in Bucket for: {gcs_file_abs_path}")
    gcs_file_pointer = conf_vars.bucket.blob(gcs_file_rel_path)
    file_found_in_bucket = gcs_file_pointer.exists()

    # Prepare list of GCS filepaths to be processed by Spark job
    conf_vars.filepaths.append(gcs_file_abs_path)

    return file_found_in_bucket


def list_files_to_process(
        arguments: argparse.Namespace,
        conf_vars: CONF_VARS) -> List[str]:
    # Goal: Get list of files to process from the input date range

    # Convert date strings to datetime
    date_format = conf_vars.ARG_DATE_FORMAT

    try:
        start_dt = datetime.strptime(arguments.DATE_START, date_format)
        end_dt = datetime.strptime(arguments.DATE_END, date_format)
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

    if conf_vars.DEBUG_ENABLED:  # In Debug Mode, process a sample file
        filenames = ["sample.csv"]

    return filenames


# _____________________________ Logging _________________

def setup_logging(
        conf_vars: configparser.ConfigParser,
        script_description: str,
        logfile_name: Optional[str] = None
) -> None:
    """
    Setup logging from config.ini
    Outputs to:
     1. console  (text in green for Transfer job)
     2. LOG_FILE is optional & in append mode
    """

    print(f"Setting up Logging for {script_description}")

    # Set Log Level
    level = conf_vars.get("LOGGING", "LOG_LEVEL", fallback="INFO")

    # Set Log-to-Console Handler
    console_handler = logging.StreamHandler()

    # Set Log-to-Console Formatter
    basic_format = conf_vars.get("LOGGING", "LOG_FORMAT")
    if "spark" in script_description.lower():
        # Case of Spark Job
        custom_formatter = logging.Formatter(
            fmt=basic_format
        )
    else:
        # Case of Transfer Job
        color_cyan = "\033[96m"  # ANSI color code
        color_reset = "\033[0m"
        custom_format = f"{color_cyan}{basic_format}{color_reset}"
        custom_formatter = logging.Formatter(
            fmt=custom_format
        )
    console_handler.setFormatter(custom_formatter)

    # Initialize list of handlers
    handlers = [console_handler]

    # Set Log-to-File Handler
    log_dir = conf_vars.get("LOGGING", "LOG_DIR", fallback=None)
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
