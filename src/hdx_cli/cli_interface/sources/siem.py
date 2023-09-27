import click

from ...library_api.utility.decorators import report_error_and_exit
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .common_commands import create as command_create
from .common_commands import any_source_impl


@click.group(help="SIEM source operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def siem(ctx: click.Context):
    any_source_impl(ctx, 'siem')


siem.add_command(command_create)
siem.add_command(command_delete)
siem.add_command(command_list)
siem.add_command(command_show)
siem.add_command(command_settings)
