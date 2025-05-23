import click

from ...library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from ...models import ProfileUserContext
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from .common_commands import any_source_impl
from .common_commands import create as command_create


@click.group(help="Kafka source operations")
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
    "--source",
    "source_name",
    help="Source for kinesis/kafka/summary/SIEM streams.",
    metavar="SOURCENAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def kafka(ctx: click.Context, project_name: str, table_name: str, source_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, kafkaname=source_name
    )
    any_source_impl(ctx, "kafka")


kafka.add_command(command_create)
kafka.add_command(command_delete)
kafka.add_command(command_list)
kafka.add_command(command_show)
kafka.add_command(command_settings)
