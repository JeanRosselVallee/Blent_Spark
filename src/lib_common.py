import os
import logging
from pathlib import Path


def setup_logging() -> None:
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
    ENV_LOG_LEVEL = os.getenv(
        "LOG_LEVEL",
        "INFO"  # Default level
    ).upper()
    level = getattr(
        logging,
        ENV_LOG_LEVEL,
        logging.INFO
    )

    # Set Log-to-Console Handler
    console_handler = logging.StreamHandler()

    # Set Log-to-Console Formatter
    color_green = "\033[92m"  # ANSI color code
    color_reset = "\033[0m"
    basic_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    custom_format = f"{color_green}{basic_format}{color_reset}"
    custom_formatter = logging.Formatter(
        fmt=custom_format
    )
    console_handler.setFormatter(custom_formatter)

    # Initialize list of handlers
    handlers = [console_handler]

    # Set Log-to-File Handler
    ENV_LOG_FILE = os.getenv("LOG_FILE")  # e.g. "logs/etl.log"
    if ENV_LOG_FILE:
        logfile_handler = logging.FileHandler(
            Path(ENV_LOG_FILE),
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
