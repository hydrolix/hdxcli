from typing import Optional
from urllib.parse import urlparse
from rich.console import Console
from rich.table import Table

import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_get,
    basic_update,
    basic_options,
)
from hdx_cli.library_api.common.exceptions import QueryOptionNotFound
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


def _build_resource_path(
        profile: ProfileUserContext,
        org_id: str,
        project_name: Optional[str],
        table_name: Optional[str],
) -> str:
    """Build the resource path based on the provided scope."""
    # Table-level scope (most specific)
    if project_name and table_name:
        _, resource_url = access_resource_detailed(
            profile, [("projects", project_name), ("tables", table_name)]
        )
        base_path = urlparse(resource_url).path
        return f"{base_path}query_options/"

    # Project-level scope
    elif project_name:
        _, resource_url = access_resource_detailed(
            profile, [("projects", project_name)]
        )
        base_path = urlparse(resource_url).path
        return f"{base_path}query_options/"

    # Organization-level scope (default)
    return f"/config/v1/orgs/{org_id}/query_options/"


@click.group(cls=HdxGroup, name="query-option")
@click.option("--project", "project_name", help="Target a specific project by name.")
@click.option("--table", "table_name", help="Target a specific table by name.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def query_option(ctx: click.Context, project_name: Optional[str], table_name: Optional[str]):
    """Manage default query options.
    This command allows you to list, set, and unset query options that will be
    applied to all queries within a specific scope.

    \b
    The scope is determined by the options provided:
    - No options: Manages options at the organization level.
    - `--project [NAME]`: Manages options for a specific project.
    - `--project [NAME] --table [NAME]`: Manages options for a specific table.
    """
    if table_name and not project_name:
        raise click.UsageError("Cannot use --table without --project.")

    profile = ctx.parent.obj["usercontext"]
    org_id = profile.org_id
    resource_path = _build_resource_path(profile, org_id, project_name, table_name)

    ctx.obj = {
        "resource_path": resource_path,
        "usercontext": profile,
    }


@click.command(cls=HdxCommand, name="set")
@click.option(
    "--option",
    "options_to_set",
    nargs=2,
    multiple=True,
    metavar="KEY VALUE",
    help="Set a single query option. Can be used multiple times.",
)
@click.option(
    "--from-file",
    "from_file_path",
    type=click.Path(exists=True, readable=True, dir_okay=False),
    help="Set query options from a JSON file.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set_(ctx: click.Context, options_to_set: tuple, from_file_path: str):
    """Set one or more query options for the specified scope.
    Options can be set individually using `--option`,
    or in bulk from a JSON file using the `--from-file` option.

    \b
    Examples:
      # Set a single option for project 'my_project'
      {full_command_prefix} --project my_project set hdx_query_max_rows 5

    \b
      # Set multiple options in one command
      {full_command_prefix} set --option hdx_query_timerange_required true --option hdx_query_max_columns_to_read 20

    \b
      # Set multiple options from a file
      {full_command_prefix} set --from-file ./options.json
    """
    if not options_to_set and not from_file_path:
        raise click.BadParameter(
         "Provide at least one --option or use the --from-file flag."
        )

    if options_to_set and from_file_path:
        raise click.BadParameter("Cannot use arguments and --from-file simultaneously.")

    profile = ctx.obj["usercontext"]
    resource_path = ctx.obj["resource_path"]
    current_settings = basic_get(profile, resource_path)
    available_options = _available_query_options(profile, resource_path)

    payload = current_settings.copy()
    if "settings" not in payload:
        payload["settings"] = {}
    if "default_query_options" not in payload["settings"]:
        payload["settings"]["default_query_options"] = {}

    if from_file_path:
        options = read_json_from_file(from_file_path)
    else:
        options = dict(options_to_set)

    # Validate all options before applying
    invalid_keys = [key for key in options if key not in available_options]
    if invalid_keys:
        raise QueryOptionNotFound(f"Invalid query option(s) {', '.join(invalid_keys)}.")

    payload["settings"]["default_query_options"].update(options)

    basic_update(profile, resource_path, body=payload)

    if from_file_path:
        msg = "Successfully set query options from file"
    else:
        msg = "Successfully set query option(s)"
    logger.info(msg)


@click.command(cls=HdxCommand)
@click.argument("query_option_name", required=False)
@click.option("--all", "all_options", is_flag=True, help="Unset all query options for the scope.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def unset(ctx: click.Context, query_option_name: Optional[str], all_options: bool):
    """Unset one or more query options for the specified scope.
    Unset a single option by name, or unset all options
    for the current scope by using the `--all` flag.

    \b
    Examples:
      # Unset a single option for project 'my_project'
      {full_command_prefix} --project my_project unset hdx_query_max_rows

    \b
      # Unset all options for the organization
      hdxcli query-option unset --all
    """
    if not query_option_name and not all_options:
        raise click.BadParameter("Provide a QUERY_OPTION_NAME or use the --all flag.")
    if query_option_name and all_options:
        raise click.BadParameter("Cannot use an argument and --all simultaneously.")

    profile = ctx.obj["usercontext"]
    resource_path = ctx.obj["resource_path"]
    current_settings = basic_get(profile, resource_path)

    options = current_settings.get("settings", {}).get("default_query_options")
    if not options:
        logger.info("No query options are configured for this scope.")
        return

    if all_options:
        options.clear()
        msg = "Successfully unset all query options"
    else:
        if query_option_name not in options:
            raise QueryOptionNotFound(f"Query option '{query_option_name}' is not set.")
        del options[query_option_name]
        msg = f"Successfully unset query option '{query_option_name}'"

    basic_update(profile, resource_path, body=current_settings)
    logger.info(msg)


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    """List the configured query options for the current scope.

    \b
    Examples:
      # List all query options for the organization
      {full_command_prefix} list

    \b
      # List all query options for 'my_project'
      {full_command_prefix} --project my_project list
    """
    profile = ctx.obj["usercontext"]
    resource_path = ctx.obj["resource_path"]
    current_settings = basic_get(profile, resource_path)
    available_options = _available_query_options(profile, resource_path)

    set_options = current_settings.get("settings", {}).get("default_query_options", {})

    if not set_options:
        logger.info("No query options are configured for this scope.")
        return

    table = Table(box=None, show_header=True, header_style="bold", padding=(0, 1), pad_edge=False)
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Set Value", justify="right")

    for name, value in set_options.items():
        opt_type = available_options.get(name, {}).get("type", "N/A")
        table.add_row(name, opt_type, str(value))

    console.print(table)


def _available_query_options(profile: ProfileUserContext, resource_path: str) -> dict:
    """Fetch available query options via OPTIONS request."""
    response = basic_options(profile, resource_path, action="PUT")
    return response.get("settings", {}).get("children", {}).get("default_query_options", {}).get("children", {})


query_option.add_command(set_)
query_option.add_command(list_)
query_option.add_command(unset)
