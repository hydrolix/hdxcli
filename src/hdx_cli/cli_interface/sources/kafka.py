import click

from ...library_api.utility.decorators import report_error_and_exit
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .common_source_actions import create as command_create
from .common_source_actions import _any_source_impl as any_source_impl


@click.group(help="Kafka source operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def kafka(ctx: click.Context):
    any_source_impl(ctx, 'kafka')


kafka.add_command(command_create)
kafka.add_command(command_delete)
kafka.add_command(command_list)
kafka.add_command(command_show)
kafka.add_command(command_settings)
