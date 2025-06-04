import click

from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.misc_operations import settings_with_force as command_settings_with_force
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create, basic_view

logger = get_logger()


@click.group(help="View-related operations")
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLENAME",
    default=None,
)
@click.option(
    "--view",
    "view_name",
    help="Explicitly pass the view name. If none is given, "
    "the default view for the used table is used.",
    metavar="viewNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def view(ctx: click.Context, project_name: str, table_name: str, view_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, viewname=view_name
    )
    basic_view(ctx)


@click.command(help="Create view.")
@click.argument("view_name", metavar="VIEWNAME", required=True)
@click.option(
    "--body-from-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    help="Path to a file containing settings for the view."
    "'name' key from the body will be replaced by the given VIEWNAME.",
    metavar="BODYFROMFILE",
    default=None,
    callback=load_json_settings_file,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, view_name: str, body_from_file: dict):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(
        user_profile,
        resource_path,
        view_name,
        body=body_from_file,
    )
    logger.info(f"Created view {view_name}")


view.add_command(create)
view.add_command(command_delete)
view.add_command(command_list)
view.add_command(command_show)
view.add_command(command_settings_with_force, "settings")
