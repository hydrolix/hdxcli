import click

from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import HdxCliException


from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings


def _any_source_impl(ctx: click.Context, source_name):
    profileinfo = ctx.parent.obj['usercontext']
    sources_path = ctx.parent.obj['resource_path']
    sourcename_resource_path =  f'{sources_path}{source_name}/'
    ctx.obj = {'usercontext': profileinfo,
                'resource_path': sourcename_resource_path}


@click.group(help="Summary source operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def summary(ctx: click.Context):
    _any_source_impl(ctx, 'summary')


summary.add_command(command_create)
summary.add_command(command_delete)
summary.add_command(command_list)
summary.add_command(command_show)
summary.add_command(command_settings)
