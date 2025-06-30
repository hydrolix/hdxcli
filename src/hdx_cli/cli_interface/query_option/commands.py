from typing import Optional
from urllib.parse import urlparse
from rich.console import Console
from rich.table import Table

import rich.box
import click

from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_get,
    basic_update,
    basic_options,
)
from hdx_cli.library_api.common.exceptions import HdxCliException, QueryOptionNotFound
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.library_api.utility.file_handling import load_json_settings_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()


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


@click.group(name="query-option")
@click.option("--project", "project_name", help="Target a specific project by name.")
@click.option("--table", "table_name", help="Target a specific table by name (requires --project).")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def query_option(ctx: click.Context, project_name: Optional[str], table_name: Optional[str]):
    """
    Manage default query options for the organization, project, or table.

    This command allows you to list, set, and unset query options
    that will be applied to all queries within a specific scope.

    \b
    The scope is determined by the options provided:
    - No options:                     Manages options at the organization level.
    - --project [NAME]:               Manages options for a specific project.
    - --project [NAME] --table [NAME]:Manages options for a specific table.

    \b
    Examples:
      # List query options set at the organization level
      hdxcli query-option list

    \b
      # Set an option for a project named 'my_project'
      hdxcli query-option --project my_project set hdx_query_max_rows 3

    \b
      # Unset all options for a table 'users' within 'my_project'
      hdxcli query-option --project my_project --table users unset --all
    """
    if table_name and not project_name:
        raise click.UsageError("Cannot use --table without --project.")

    profile = ctx.parent.obj["usercontext"]
    org_id = profile.org_id
    resource_path = _build_resource_path(profile, org_id, project_name, table_name)

    # Fetch common data once to avoid redundant API calls
    current_settings = basic_get(profile, resource_path)
    available_options = _available_query_options(profile, resource_path)

    if not available_options:
        raise HdxCliException("Failed to retrieve available query options.")

    ctx.obj = {
        "resource_path": resource_path,
        "usercontext": profile,
        "current_settings": current_settings,
        "available_options": available_options,
    }


@click.command(name="set")
@click.argument("query_option_name", required=False)
@click.argument("query_option_value", required=False)
@click.option(
    "--from-file",
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
    help="Set query options from a JSON file.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set_(ctx: click.Context, query_option_name: str, query_option_value: str, from_file: dict):
    """
    Set one or more query options for the specified scope.

    Options can be set individually by providing a name and a value,
    or in bulk from a JSON file using the --from-file option.

    \b
    Examples:
      # Set a single option for project 'my_project'
      hdxcli query-option --project my_project set hdx_query_max_rows 5

    \b
      # Set multiple options from a file for the organization
      hdxcli query-option set --from-file ./options.json
    """
    if not (query_option_name and query_option_value) and not from_file:
        raise click.BadParameter(
            "Provide either QUERY_OPTION_NAME and QUERY_OPTION_VALUE, or --from-file."
        )

    if query_option_name and from_file:
        raise click.BadParameter("Cannot use arguments and --from-file simultaneously.")

    profile = ctx.obj["usercontext"]
    resource_path = ctx.obj["resource_path"]
    current_settings = ctx.obj["current_settings"]
    available_options = ctx.obj["available_options"]

    payload = current_settings.copy()
    if "settings" not in payload:
        payload["settings"] = {}
    if "default_query_options" not in payload["settings"]:
        payload["settings"]["default_query_options"] = {}

    options_to_set = from_file if from_file else {query_option_name: query_option_value}

    # Validate all options before applying
    invalid_keys = [key for key in options_to_set if key not in available_options]
    if invalid_keys:
        raise QueryOptionNotFound(f"Invalid query option(s) {', '.join(invalid_keys)}.")

    payload["settings"]["default_query_options"].update(options_to_set)

    basic_update(profile, resource_path, body=payload)

    if from_file:
        msg = "Successfully set query options from file"
    else:
        msg = f"Successfully set query option '{query_option_name}' to '{query_option_value}'"
    logger.info(msg)


@click.command()
@click.argument("query_option_name", required=False)
@click.option("--all", "all_options", is_flag=True, help="Unset all query options for the scope.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def unset(ctx: click.Context, query_option_name: Optional[str], all_options: bool):
    """
    Unset one or more query options for the specified scope.

    Unset a single option by providing its name, or unset all options
    for the current scope by using the --all flag.

    \b
    Examples:
      # Unset a single option for project 'my_project'
      hdxcli query-option --project my_project unset hdx_query_max_rows

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
    current_settings = ctx.obj["current_settings"]

    try:
        options = current_settings["settings"]["default_query_options"]
        if all_options:
            options.clear()
            msg = "Successfully unset all query options"
        else:
            del options[query_option_name]
            msg = f"Successfully unset query option '{query_option_name}'"


        basic_update(profile, resource_path, body=current_settings)
        logger.info(msg)
    except KeyError:
        raise QueryOptionNotFound(f"Query option '{query_option_name}' is not set.")
    except (TypeError, KeyError):
        logger.info("No query options were set for this resource.")


@click.command(name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    """
    List the configured query options for the current scope.

    Displays a table of all query options that have been explicitly set
    """
    current_settings = ctx.obj["current_settings"]
    available_options = ctx.obj["available_options"]

    set_options = current_settings.get("settings", {}).get("default_query_options", {})

    if not set_options:
        logger.info("No query options are configured for this scope.")
        return

    table = Table(
        box=rich.box.HORIZONTALS,
        show_edge=False,
        show_lines=False
    )
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Set Value", justify="right")

    for name, value in set_options.items():
        opt_type = available_options.get(name, {}).get("type", "N/A")
        table.add_row(name, opt_type, str(value))

    console = Console()
    console.print(table)


def _available_query_options(profile: ProfileUserContext, resource_path: str) -> dict:
    """Fetch available query options via OPTIONS request."""
    response = basic_options(profile, resource_path, action="PUT")
    return response.get("settings", {}).get("children", {}).get("default_query_options", {}).get("children", {})


query_option.add_command(set_)
query_option.add_command(list_)
query_option.add_command(unset)
