import logging
from typing import Dict, List, Optional, Tuple

import click
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
from rich.console import Console
from rich.text import Text

from hdx_cli.library_api.common.exceptions import LogsQueryException, LogsQueryTimeoutException
from hdx_cli.models import ProfileUserContext

logging.getLogger("clickhouse_connect").setLevel(logging.CRITICAL)
PAGER_THRESHOLD = 20
DEFAULT_CLICKHOUSE_PORT = 8088
console = Console(soft_wrap=True)

# Static list of levels for user input validation
LEVELS = ["INFO", "WARN", "ERROR", "TRACE", "FATAL", "NONE"]

# Mapping of levels
LEVEL_VARIATIONS = {
    "INFO": ("info", "INFO"),
    "WARN": ("warn", "WARN", "warning", "WARNING"),
    "ERROR": ("error", "ERROR"),
    "FATAL": ("fatal", "FATAL"),
    "TRACE": ("trace", "TRACE"),
}


def _build_query(
    service: Optional[str],
    level: Optional[str],
    text_filter: Optional[str],
    tail: Optional[int],
) -> Tuple[str, Dict]:
    """
    Constructs a parameterized query template and a parameter's dictionary.
    """
    select_clause = (
        "SELECT timestamp, level, `kubernetes.container_name` AS service, message, error"
    )
    params = {}
    conditions = []

    if service:
        conditions.append("service = %(service_name)s")
        params["service_name"] = service

    if level:
        if level == "NONE":
            conditions.append("level IS NULL")
        else:
            variations = LEVEL_VARIATIONS.get(level, ())
            if variations:
                params["level_variations"] = tuple(variations)
                conditions.append("level IN %(level_variations)s")

    if text_filter:
        params["text_pattern"] = f"%{text_filter}%"
        or_clause = "(message LIKE %(text_pattern)s OR error LIKE %(text_pattern)s)"
        conditions.append(or_clause)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    limit_clause = "LIMIT %(limit_val)s"
    params["limit_val"] = tail

    template = (
        f"{select_clause} FROM hydro.logs {where_clause} ORDER BY timestamp DESC {limit_clause}"
    )
    return template, params


def _execute_query(
    profile: ProfileUserContext,
    query_template: str,
    params: Dict,
) -> List[Dict]:
    hostname = profile.hostname
    base_host = hostname.split(":", 1)[0]

    try:
        client = clickhouse_connect.get_client(
            host=base_host,
            port=DEFAULT_CLICKHOUSE_PORT,
            username="bearer",
            password=profile.token,
            secure=True,
            send_receive_timeout=profile.timeout,
        )
        result = client.query(query_template, parameters=params)

        return [dict(zip(result.column_names, row)) for row in result.result_rows]
    except ClickHouseError as e:
        if "timed out" in str(e).lower():
            raise LogsQueryTimeoutException(timeout=profile.timeout) from e
        else:
            error_message = f"The logs query failed.\n  └─ Reason: {e}"
            raise LogsQueryException(error_message) from e


def _format_logs(logs: List[Dict]) -> List[Text]:
    formatted_logs = []
    for log in logs:
        log_level_original = log.get("level")
        log_level_normalized = log_level_original.upper() if log_level_original else "NONE"

        timestamp_dt = log.get("timestamp")
        timestamp_str = (
            timestamp_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if timestamp_dt else " " * 23
        )

        service = log.get("service", "n/a")
        message = log.get("message", "")
        error_details = log.get("error")

        sanitized_message = message.replace("\r", "").strip() if message else ""

        line = Text.assemble(
            (f"{timestamp_str} ", "dim"),
            (f"[{log_level_normalized}] ", "bold"),
            (f"[{service}] ", "green"),
            sanitized_message,
        )

        if error_details:
            # Append the error line if exists.
            sanitized_error = str(error_details).replace("\r", "").strip()
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
    service: Optional[str],
    tail: Optional[int],
    level: Optional[str],
    filter_pattern: Optional[str],
):
    """Main orchestrator for the 'logs show' command logic."""
    with console.status("[dim]Querying and processing logs...[/]"):
        query_template, params = _build_query(service, level, filter_pattern, tail)
        logs_from_api = _execute_query(profile, query_template, params)
        formatted_logs = _format_logs(logs_from_api)

    _display_logs(formatted_logs)
