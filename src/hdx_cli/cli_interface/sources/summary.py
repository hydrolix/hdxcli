import click

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.context import ProfileUserContext
from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .common_commands import any_source_impl


@click.group(help="Summary source operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--source', 'source_name', help='Source for kinesis/kafka/summary/SIEM streams.',
              metavar='SOURCENAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def summary(ctx: click.Context,
            project_name,
            table_name,
            source_name):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name,
                                      summaryname=source_name)
    any_source_impl(ctx, 'summary')


summary.add_command(command_create)
summary.add_command(command_delete)
summary.add_command(command_list)
summary.add_command(command_show)
summary.add_command(command_settings)
