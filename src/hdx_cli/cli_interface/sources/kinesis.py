import click

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.context import ProfileUserContext
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .common_commands import create as command_create
from .common_commands import any_source_impl


@click.group(help="Kinesis source operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--source', 'source_name', help='Source for kinesis/kafka/summary/SIEM streams.',
              metavar='SOURCENAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def kinesis(ctx: click.Context,
            project_name,
            table_name,
            source_name):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name,
                                      kinesisname=source_name)
    any_source_impl(ctx, 'kinesis')


kinesis.add_command(command_create)
kinesis.add_command(command_delete)
kinesis.add_command(command_list)
kinesis.add_command(command_show)
kinesis.add_command(command_settings)
