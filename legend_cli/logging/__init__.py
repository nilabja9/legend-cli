"""CLI run logging module for Legend CLI.

Provides database logging for CLI command runs to help with
debugging and auditing.
"""

from legend_cli.logging.cli_db import CLIRunDatabase, get_default_cli_db_path
from legend_cli.logging.cli_service import (
    CLIRunLogger,
    get_cli_logger,
    log_cli_run,
)

__all__ = [
    "CLIRunDatabase",
    "get_default_cli_db_path",
    "CLIRunLogger",
    "get_cli_logger",
    "log_cli_run",
]
