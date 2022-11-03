"""Commands relative to project handling  operations"""
import click

from ...library_api.common.generic_resource import access_resource

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import HdxCliException


from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings


@click.group(help="Dictionary-related operations")
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def dictionary(ctx: click.Context):
    profileinfo = ctx.parent.obj['usercontext']
    org_id = profileinfo.org_id
    project_id = access_resource(profileinfo,
                                 [('projects', profileinfo.projectname)])['uuid']
    resource_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/dictionaries'
    ctx.obj = {'resource_path': resource_path,
               'usercontext': profileinfo}


dictionary.add_command(command_create)
dictionary.add_command(command_list)
dictionary.add_command(command_delete)
dictionary.add_command(command_show)
dictionary.add_command(command_settings)
