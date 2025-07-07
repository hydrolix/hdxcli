import click

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_column, basic_create
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    skip_group_logic_on_help,
    report_error_and_exit,
    ensure_logged_in
)
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@click.group(name="column")
@click.option(
    "--project",
    "project_name",
    help="Target project for the column operation.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Target table for the column operation.",
    metavar="TABLENAME",
    default=None,
)
@click.option(
    "--column",
    "column_name",
    help="Target column for the operation.",
    metavar="COLUMNNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def column(ctx: click.Context, project_name: str, table_name: str, column_name: str):
    """
    Manage all aspects of table columns.

    \b
    Provides commands to list all columns, show details for a specific
    one, or delete any existing alias column. It also includes specialized
    commands to create new alias columns from an expression (`add-alias`)
    and to add alternative names to existing columns (`add-name`).

    \b
    A project and table context is required for all operations.

    \b
    Examples:
      # List all columns in the 'my_table' table of 'my_project'
      hdxcli column --project my_project --table my_table list

    \b
      # Show details for the 'email' column
      hdxcli column --project my_project --table my_table --column email show

    \b
      # Add an alias 'user_email' to the 'email' column
      hdxcli column --project my_project --table my_table --column email add-name user_email
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, columnname=column_name
    )
    basic_column(ctx)


@click.command(name="add-name")
@click.argument("new_name", metavar="NEWNAME", required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def add_name(ctx: click.Context, new_name: str):
    """
    Add a new name to a column.

    This allows a column to be referenced by an additional name.
    Requires --project, --table, and --column options to be set on the parent command.

    \b
    Example:
      # Add 'user_id' as an alternative name for the 'user' column
      hdxcli column --project my_project --table my_table --column user add-name user_id
    """
    parent_obj = ctx.parent.obj
    column_details = parent_obj.get("specific_resource")

    if not (column_details and (column_id := column_details.get("id"))):
        raise click.UsageError(
            "The '--column' option is required for this command. "
            "Ensure a valid column is specified via '--project', '--table', and '--column'."
        )

    user_profile = parent_obj["usercontext"]
    base_path = parent_obj["resource_path"]
    resource_path = f"{base_path}{column_id}/add_name/"

    basic_create(
        user_profile,
        resource_path,
        body={"new_name": new_name},
    )
    column_name = column_details.get("name", column_id)
    logger.info(f"Added new name '{new_name}' to column '{column_name}'")


@click.command(name="add-alias")
@click.argument("alias_name", metavar="ALIASNAME", required=True)
@click.argument("expression", metavar="EXPRESSION", required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def add_alias(ctx: click.Context, alias_name: str, expression: str):
    """
    Add a new ALIAS column to a table.

    This creates a new column defined by an expression.
    Requires --project and --table options to be set on the parent command.

    \b
    Example:
      # Add an alias column 'full_name' by concatenating 'first_name' and 'last_name'
      hdxcli column --project my_project --table my_table add-alias full_name "concat(first_name, ' ', last_name)"
    """
    parent_obj = ctx.parent.obj
    user_profile = parent_obj.get("usercontext")
    resource_path = parent_obj.get("resource_path")

    if not (user_profile and resource_path):
        raise click.UsageError(
            "The '--project' and '--table' options are required for this command."
        )

    body = {
        "name": alias_name,
        "expression": expression,
    }

    basic_create(
        user_profile,
        resource_path,
        body=body,
    )
    table_name = user_profile.tablename
    logger.info(f"Added new alias column '{alias_name}' to table '{table_name}'")


column.add_command(add_name)
column.add_command(add_alias)
column.add_command(command_list)
column.add_command(command_show)
column.add_command(command_delete)
