"""Commands relative to project resource."""
import click

from ..common.undecorated_click_commands import basic_create
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.context import ProfileUserContext
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show,
                                      activity as command_activity,
                                      stats as command_stats)
from ..common.misc_operations import settings as command_settings


@click.group(help="Project-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.pass_context
def project(ctx: click.Context,
            project_name):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/',
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name)


@click.command(help='Create project.')
@click.argument('project_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           project_name: str):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    basic_create(user_profile,
                 resource_path,
                 project_name,
                 None,
                 None)
    print(f'Created project {project_name}')


project.add_command(command_list)
project.add_command(create)
project.add_command(command_delete)
project.add_command(command_show)
project.add_command(command_settings)
project.add_command(command_activity)
project.add_command(command_stats)
