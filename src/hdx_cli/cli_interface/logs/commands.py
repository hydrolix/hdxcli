from typing import Optional

import click

from hdx_cli.library_api.utility.decorators import ensure_logged_in, report_error_and_exit

from .logic import show_logs_logic


@click.group()
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def logs(ctx: click.Context):
    """A group of commands for retrieving and displaying logs."""
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = "/config/v1/cluster_logs/"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@logs.command(name="show")
@click.option(
    "--service",
    "-s",
    "service_name",
    help="Filter logs by a specific service name (server-side).",
    metavar="<name>",
)
@click.option(
    "--tail",
    "-t",
    "tail_count",
    type=click.IntRange(min=0, max=100000),
    help="Show the last N log entries (server-side). Default is 1000.",
    metavar="<N>",
)
@click.option(
    "--level",
    "-l",
    "level_filter",
    help="Filter logs by level (e.g., INFO, ERROR, NONE). "
    "Match is exact and case-insensitive (client-side).",
    metavar="<level>",
)
@click.option(
    "--filter",
    "-f",
    "filter_pattern",
    help="Search for a text pattern within the log message (client-side).",
    metavar="<pattern>",
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
    """Display and filter logs. This command fetches a collection of logs
    and configurations from the cluster, allowing for powerful server-side
    and client-side filtering to help developers debug issues efficiently.

    Server-side filters (`--service`, `--tail`) are applied by the API before
    sending the data, making the request faster. Client-side filters
    (`--level`, `--filter`) are applied by the CLI after receiving the data.

    NOTE: When combining server-side and client-side filters, the final
    number of logs displayed may be less than the count specified with `--tail`.
    For example, `--tail 100 --level error` will fetch the last 100 logs
    and then display only those that are errors.

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
