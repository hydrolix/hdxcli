import re
from typing import Optional

import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.library_api.utility.decorators import ensure_logged_in, report_error_and_exit

from .query import LEVELS, show_logs_logic


def _validate_level(ctx, param, value):
    """Validate and normalize the log level."""
    if not value:
        return None

    for level in LEVELS:
        if level.lower() == value.lower():
            # Return the uppercase version
            return level

    raise click.BadParameter(f"'{value}' is not a valid level. Choose from: {', '.join(LEVELS)}")


def _validate_service_name(ctx, param, value):
    """Validate that the service name contains only safe characters."""
    if not value:
        return None
    if not re.match(r"^[a-zA-Z0-9_.-]+$", value):
        raise click.BadParameter(
            f"'{value}' contains invalid characters. Use only alphanumeric, '_', '.', and '-'."
        )
    return value


@click.group(cls=HdxGroup)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def logs(ctx: click.Context):
    """Provides commands to interact with logs from the cluster."""
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = "/query/"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@logs.command(cls=HdxCommand, name="show")
@click.option(
    "--service",
    "-s",
    "service_name",
    help="Filter logs by a specific service name.",
    callback=_validate_service_name,
)
@click.option(
    "--tail",
    "-t",
    "tail_count",
    type=click.IntRange(min=1, max=10000),
    default=10,
    help="Show the last N log entries. Defaults to 10.",
)
@click.option(
    "--level",
    "-l",
    "level_filter",
    help=f"Filter by log level. Case-insensitive. Choices: {', '.join(LEVELS)}.",
    callback=_validate_level,
)
@click.option(
    "--filter",
    "-f",
    "filter_pattern",
    help="Search for a text pattern within the log message.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(
    ctx: click.Context,
    service_name: Optional[str],
    tail_count: Optional[int],
    level_filter: Optional[str],
    filter_pattern: Optional[str],
):
    """Display and filter diagnostic logs from the cluster.
    This command queries the logs table directly, allowing
    filtering to help developers debug issues efficiently.

    \b
    Examples:
      # Show the last 100 logs from the 'query-peer' service
      hdxcli logs show --service query-peer --tail 100

    \b
      # Show all ERROR logs, searching for the term 'segmentation fault'
      hdxcli logs show --level ERROR --filter "segmentation fault"

    \b
      # Show logs that do not have a level assigned
      hdxcli logs show --level none
    """
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    show_logs_logic(profile, resource_path, service_name, tail_count, level_filter, filter_pattern)
