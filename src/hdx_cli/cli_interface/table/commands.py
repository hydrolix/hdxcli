"""Commands relative to project handling  operations"""
import click
import json
import requests
from datetime import datetime
from functools import lru_cache

from ...library_api.common import auth as auth_api
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import HdxCliException, LogicException

from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings



@click.group(help="Table-related operations")
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def table(ctx: click.Context):
    profileinfo = ctx.parent.obj['usercontext']
    hostname = profileinfo.hostname
    project_name = profileinfo.projectname
    if not project_name:
        raise LogicException(f"No project parameter was provided and no project is set in profile '{profileinfo.profilename}'")
    org_id = profileinfo.org_id
    list_projects_url = f'https://{hostname}/config/v1/orgs/{org_id}/projects/'
    auth_token: AuthInfo = profileinfo.auth
    headers = {'Authorization': f'{auth_token.token_type} {auth_token.token}',
               'Accept': 'application/json'}
    try:
        projects_list = rest_ops.list(list_projects_url,
                                      headers=headers)
        project_id = [p['uuid'] for p in projects_list if p['name'] == project_name]
        ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/{project_id[0]}/tables/',
                   'usercontext': profileinfo}
    except IndexError as idx_err:
        raise LogicException(f'Cannot find project: {project_name}') from idx_err


table.add_command(command_create)
table.add_command(command_delete)
table.add_command(command_list)
table.add_command(command_show)
table.add_command(command_settings)
