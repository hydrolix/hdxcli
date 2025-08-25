import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from hdx_cli.models import ProfileUserContext

from .common_commands import any_source_impl
from .common_commands import create as command_create


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
    "--source",
    "source_name",
    help="The name of the SIEM source.",
    metavar="SIEM_SOURCE_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def siem(ctx: click.Context, project_name: str, table_name: str, source_name: str):
    """Manage SIEM sources."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, siemname=source_name
    )
    any_source_impl(ctx, "siem")


siem.add_command(command_create)
siem.add_command(command_delete)
siem.add_command(command_list)
siem.add_command(command_show)
siem.add_command(command_settings)
