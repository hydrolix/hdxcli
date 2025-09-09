import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_view
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from hdx_cli.library_api.utility.file_handling import read_json_from_file
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
    "--view",
    "view_name",
    help="Explicitly pass the view name. If none is given, "
    "the default view for the table is used.",
    metavar="VIEW_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def view(ctx: click.Context, project_name: str, table_name: str, view_name: str):
    """This group of commands allows to create, list, show, delete, and
    manage settings for views. A project and table context is required
    for all operations."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, viewname=view_name
    )
    basic_view(ctx)


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--body-from-file",
    "-f",
    "settings_filename",
    type=click.Path(exists=True, readable=True),
    help="Path to a JSON file with view settings.",
    required=True,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, resource_name: str, settings_filename: str):
    """Creates a new {resource} from a JSON configuration file
    in the specified project and table.

    \b
    Examples:
      # Create a new {resource} from a JSON configuration file
      {full_command_prefix} create {example_name} --body-from-file path/to/your-view.json
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    settings_body = read_json_from_file(settings_filename)
    basic_create(
        user_profile,
        resource_path,
        resource_name,
        body=settings_body,
    )
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


view.add_command(create)
view.add_command(command_delete)
view.add_command(command_list)
view.add_command(command_show)
view.add_command(command_settings)
