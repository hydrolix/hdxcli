"""Commands relative to project resource."""
import click

from ..common import rest_operations as ro
from ..common.undecorated_click_commands import basic_create

from ...library_api.common.rest_operations import create
from ...library_api.common.config_constants import HDX_CLI_HOME_DIR
from ...library_api.common.dates import get_datetime_from_formatted_string
from ...library_api.common.auth import AuthInfo, load_profile
from ...library_api.common.context import ProfileLoadContext
from ...library_api.common.exceptions import TokenExpiredException, HdxCliException
from ...library_api.utility.decorators import report_error_and_exit


from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings


@click.group(help="Project-related operations")
@click.pass_context
def project(ctx: click.Context):
    profile = ctx.parent.obj['usercontext']
    org_id = profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/',
               'usercontext': profile}



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
    print(f'Created project {project_name}.')


project.add_command(command_list)
project.add_command(create)
project.add_command(command_delete)
project.add_command(command_show)
project.add_command(command_settings)
