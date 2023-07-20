import click

from ...library_api.utility.decorators import report_error_and_exit
from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .common_source_actions import _any_source_impl as any_source_impl


@click.group(help="Summary source operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def summary(ctx: click.Context):
    any_source_impl(ctx, 'summary')


summary.add_command(command_create)
summary.add_command(command_delete)
summary.add_command(command_list)
summary.add_command(command_show)
summary.add_command(command_settings)
