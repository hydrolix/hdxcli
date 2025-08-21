import json

import click
from rich.console import Console
from rich.table import Table

from hdx_cli.cli_interface.common.cached_operations import find_rowpolicies
from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create,
    basic_row_policy,
    basic_show,
)
from hdx_cli.library_api.common.exceptions import CommandLineException, LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECT_NAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLE_NAME",
    default=None,
)
@click.option(
    "--row-policy",
    "row_policy_name",
    help="Explicitly pass the row policy name.",
    metavar="ROW_POLICY_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def row_policy(ctx: click.Context, project_name: str, table_name: str, row_policy_name: str):
    """Manages Row-Level security policies for tables.

    This command group provides functionality to create, list, show, delete,
    and manage roles for row policies, allowing for fine-grained
    access control over the data within a table.
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile,
        projectname=project_name,
        tablename=table_name,
        rowpolicyname=row_policy_name,
    )
    basic_row_policy(ctx)


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--filter",
    "filter_expression",
    help="The filter expression for the policy (e.g., '\"claimed\"=false').",
    metavar="FILTER_EXPRESSION",
    required=True,
)
@click.option(
    "--restrictive",
    is_flag=True,
    default=False,
    help="Set the policy as restrictive. Default is permissive.",
)
@click.option(
    "--role",
    "roles",
    multiple=True,
    metavar="ROLE_NAME",
    help="Role to associate with this policy. Can be specified multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    filter_expression: str,
    restrictive: bool,
    roles: list[str],
):
    """Creates a new {resource} for the specified table.
    A {resource} filters the data that users can see based on a filter
    expression. It must be associated with at least one role to take effect.

    \b
    Examples:
      # Create a PERMISSIVE {resource} to show logs from Europe to 'analyst' users
      hdxcli row-policy --project hydro --table logs create europe_logs --filter "region = 'EU'" --role analyst

    \b
      # Create a RESTRICTIVE {resource} to ensure only non-draft documents are ever shown
      hdxcli row-policy --project docs --table articles create ensure_published --filter "status != 'draft'" --restrictive
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    body = {
        "name": resource_name,
        "filter": filter_expression,
        "restrictive": restrictive,
        "roles": roles if roles else [],
    }

    basic_create(user_profile, resource_path, resource_name, body=body)
    logger.info(f"Created row policy {resource_name}")


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    """Lists all {resource_plural} for a given table.
    Displays a summary of all {resource_plural}, including their name, filter
    expression, whether they are restrictive, and their associated roles.

    \b
    Examples:
      # List all {resource_plural}
      {full_command_prefix} list
    """
    user_profile = ctx.parent.obj["usercontext"]
    policies = find_rowpolicies(user_profile)

    if not policies:
        project = user_profile.projectname
        table = user_profile.tablename
        logger.info(f"No row policy found for table '{project}.{table}'.")
        return

    console = Console()
    table = Table(show_header=True, box=None, header_style="bold", pad_edge=False)
    table.add_column("Name", min_width=15)
    table.add_column("Filter", min_width=25, overflow="fold")
    table.add_column("Restrictive")
    table.add_column("Roles")

    for policy in policies:
        # Format roles array into a comma-separated string
        roles_str = ", ".join(policy.get("roles", []))
        # Format restrictive boolean into a simple Yes/No
        restrictive_str = "Yes" if policy.get("restrictive") else "No"

        table.add_row(
            policy.get("name", ""),
            policy.get("filter", ""),
            restrictive_str,
            roles_str,
        )
    console.print(table)


def _role_operation(ctx: click.Context, policy_name: str, roles: list[str], operation: str):
    """Internal helper to handle both add_roles and remove_roles operations."""
    if not roles:
        raise CommandLineException("At least one --role must be provided.")

    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    row_policy_json = json.loads(basic_show(user_profile, resource_path, policy_name))
    row_policy_id = row_policy_json.get("uuid")
    if not row_policy_id:
        raise LogicException(f"There was an error with the row policy '{policy_name}'.")

    body = {"roles": list(roles)}

    resource_path = f"{resource_path}{row_policy_id}/{operation}/"
    basic_create(user_profile, resource_path, body=body)

    op_str = "Added" if operation == "add_roles" else "Removed"
    logger.info(f"{op_str} roles for row policy '{policy_name}'")


@click.command(cls=HdxCommand, name="add-role")
@click.argument("resource_name")
@click.option(
    "--role",
    "roles",
    multiple=True,
    required=True,
    metavar="ROLE_NAME",
    help="Role to add. Can be specified multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def add_role(ctx: click.Context, resource_name: str, roles: list[str]):
    """Adds one or more roles to an existing {resource}.
    This command associates roles with a {resource}, granting the
    permissions defined by that {resource} to users who have those roles.

    \b
    Examples:
      # Add the 'viewer' role to the 'europe_logs' {resource}
      hdxcli row-policy --project hydro --table logs add-role europe_logs --role viewer

    \b
      # Add multiple roles at once
      hdxcli row-policy --project hydro --table logs add-role europe_logs --role viewer --role editor
    """
    _role_operation(ctx, resource_name, roles, "add_roles")


@click.command(cls=HdxCommand, name="remove-role")
@click.argument("resource_name")
@click.option(
    "--role",
    "roles",
    multiple=True,
    required=True,
    metavar="ROLE_NAME",
    help="Role to remove. Can be specified multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove_role(ctx: click.Context, resource_name: str, roles: list[str]):
    """Removes one or more roles from an existing {resource}.
    This command disassociates roles from a {resource}, revoking the
    permissions defined by that {resource} from users who have those roles.

    \b
    Examples:
      # Remove the 'editor' role from the 'europe_logs' policy
      hdxcli row-policy --project hydro --table logs remove-role europe_logs --role editor
    """
    _role_operation(ctx, resource_name, roles, "remove_roles")


row_policy.add_command(create)
row_policy.add_command(list_)
row_policy.add_command(command_show)
row_policy.add_command(command_delete)
row_policy.add_command(command_settings)
row_policy.add_command(add_role)
row_policy.add_command(remove_role)
