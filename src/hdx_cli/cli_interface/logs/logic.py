import ast
import io
import json
import zipfile
from typing import List, Optional

from rich.console import Console
from rich.text import Text

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_get
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.models import ProfileUserContext

# Threshold for when to engage the pager
PAGER_THRESHOLD = 20

console = Console()


def _fetch_and_parse_logs(
    profile: ProfileUserContext,
    resource_path: str,
    service: Optional[str],
    tail: Optional[int],
) -> List[str]:
    """
    Fetches the logs zip from the API, builds the request with
    server-side filters, unzips the content in-memory, and parses it.
    """
    params = {}
    if service:
        params["pod_name"] = service
    if tail is not None:
        params["results"] = tail

    zip_content = basic_get(profile, resource_path, fmt="content", **params)

    if not isinstance(zip_content, bytes):
        raise LogicException("The API response for logs was not in the expected binary format.")

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content), "r") as zip_ref:
            file_list = zip_ref.namelist()
            if not file_list:
                raise LogicException("The fetched ZIP archive is empty.")
            log_data_str = zip_ref.read(file_list[0]).decode("utf-8")

        parsed_data = ast.literal_eval(log_data_str)
        if "logs" not in parsed_data:
            raise LogicException("Key 'logs' not found in the parsed data.")
        return parsed_data["logs"]
    except (zipfile.BadZipFile, IndexError):
        raise LogicException("Failed to read or decompress the log data from the API response.")
    except (ValueError, SyntaxError):
        raise LogicException("Failed to parse the log content. The data may be malformed.")


def _filter_and_format_logs(
    logs: List[str],
    level_filter: Optional[str],
    text_filter: Optional[str],
) -> List[str]:
    """
    Parses the log string, applies client-side filters,
    and formats the logs for display.
    """
    if not logs or not isinstance(logs[0], str):
        # Handle cases where the logs list is empty or has an unexpected format
        return []

    # Extract the single string and parse it as JSON
    try:
        giant_log_string = logs[0]
        actual_logs = json.loads(giant_log_string)
    except (json.JSONDecodeError, IndexError):
        raise LogicException("Failed to parse the log string as JSON.")

    filtered_logs = []
    target_level = level_filter.upper() if level_filter else None

    # Iterate through the actual list of log dictionaries
    for log in actual_logs:
        log_level_original = log.get("level")
        log_level_normalized = (
            log_level_original.upper() if isinstance(log_level_original, str) else "NONE"
        )

        if target_level and log_level_normalized != target_level:
            continue

        message = log.get("message", "")
        if text_filter and text_filter.lower() not in message.lower():
            continue

        timestamp = log.get("timestamp", " " * 19)
        service = log.get("kubernetes.container_name", "n/a")

        formatted_line = Text.assemble(
            (f"{timestamp} ", "dim"),
            (f"[{log_level_normalized:}] ", "bold"),
            (f"[{service}] ", "green"),
            message,
        )
        filtered_logs.append(formatted_line)

    return filtered_logs


def _display_logs(formatted_logs: List[str]):
    """
    Displays the logs, using a pager if the number of logs exceeds the threshold.
    """
    if not formatted_logs:
        console.print("No logs to display with the current filters.")
        return

    if len(formatted_logs) > PAGER_THRESHOLD:
        with console.pager():
            for line in formatted_logs:
                console.print(line)
    else:
        for line in formatted_logs:
            console.print(line)


def show_logs_logic(
    profile: ProfileUserContext,
    resource_path: str,
    service: Optional[str],
    tail: Optional[int],
    level: Optional[str],
    filter_pattern: Optional[str],
):
    """Main orchestrator for the 'logs show' command logic."""
    with console.status("[bold cyan]Fetching and processing logs...[/]"):
        logs_from_api = _fetch_and_parse_logs(profile, resource_path, service, tail)
        formatted_logs = _filter_and_format_logs(logs_from_api, level, filter_pattern)

    _display_logs(formatted_logs)
