import csv
import io
from typing import Dict, List, Optional

import click
from rich.console import Console
from rich.text import Text

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_get
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.models import ProfileUserContext

PAGER_THRESHOLD = 20
console = Console(soft_wrap=True)


def _build_query(
    service: Optional[str],
    level: Optional[str],
    text_filter: Optional[str],
    tail: Optional[int],
) -> str:
    """Constructs the SQL query string based on the provided filters."""
    select_clause = "SELECT timestamp, level, kubernetes.container_name AS service, message, error"

    conditions = []
    if service:
        conditions.append(f"service = '{service}'")
    if level:
        conditions.append(f"lower(level) = '{level.lower()}'")
    if text_filter:
        conditions.append(f"message LIKE '%{text_filter}%'")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    limit_clause = f"LIMIT {tail}"

    query = f"{select_clause} FROM hydro.logs {where_clause} ORDER BY timestamp DESC {limit_clause} FORMAT CSVWithNames"
    return query


def _execute_query(
    profile: ProfileUserContext,
    resource_path: str,
    query: str,
) -> List[Dict]:
    """
    Executes the query against the API, receives CSV data,
    and parses it into a list of dictionaries.
    """
    response_data = basic_get(profile, resource_path, fmt="verbatim", query=query)

    if not isinstance(response_data, bytes):
        raise LogicException("The query API did not return bytes as expected.")

    try:
        decoded_csv = response_data.decode("utf-8")
        csv_file = io.StringIO(decoded_csv)
        reader = csv.DictReader(csv_file)
        logs = list(reader)
        return logs
    except (UnicodeDecodeError, csv.Error) as e:
        raise LogicException(f"Failed to parse the CSV response from the API: {e}")


def _format_logs(logs: List[Dict]) -> List[Text]:
    """Formats the log dictionaries into rich Text objects for display."""
    formatted_logs = []
    for log in logs:
        log_level_original = log.get("level")
        log_level_normalized = (
            log_level_original.upper()
            if log_level_original and log_level_original != r"\N"
            else "NONE"
        )

        timestamp = log.get("timestamp", " " * 19)
        service_original = log.get("service")
        message_original = log.get("message", "")
        error_details = log.get("error")

        service = "n/a" if not service_original or service_original == r"\N" else service_original
        message = "" if message_original == r"\N" else message_original
        sanitized_message = message.replace("\r", "").strip()

        line = Text.assemble(
            (f"{timestamp} ", "dim"),
            (f"[{log_level_normalized}] ", "bold"),
            (f"[{service}] ", "green"),
            sanitized_message,
        )

        if error_details and error_details.strip() and error_details != r"\N":
            sanitized_error = error_details.replace("\r", "").strip()
            # Append the error line if exists.
            error_line = Text(f"\n  └─ Error: {sanitized_error}", style="red")
            line.append(error_line)

        formatted_logs.append(line)

    return formatted_logs


def _display_logs(formatted_logs: List[Text]):
    """
    Displays the logs, capturing the output and using click's pager
    if the number of logs exceeds the threshold.
    """
    if not formatted_logs:
        console.print("No logs to display with the current filters.")
        return

    formatted_logs.reverse()

    if len(formatted_logs) > PAGER_THRESHOLD:
        # Print to the main console inside the 'with' block.
        with console.capture() as capture:
            for line in formatted_logs:
                console.print(line)

        # Get the captured string and pass it to click's pager.
        output = capture.get()
        click.echo_via_pager(output)
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
    with console.status("[dim]Querying and processing logs...[/]"):
        query = _build_query(service, level, filter_pattern, tail)
        logs_from_api = _execute_query(profile, resource_path, query)
        formatted_logs = _format_logs(logs_from_api)

    _display_logs(formatted_logs)
