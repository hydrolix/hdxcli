"""Transform resource command. It flows down url for current table"""
import json
import requests
from datetime import datetime
from functools import lru_cache

import click

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common import auth as auth_api
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.exceptions import (HdxCliException,
                                              TransformFileNotFoundException,
                                              LogicException,
                                              TransformNotFoundException)

from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings


@click.group(help="Transform-related operations")
@click.option('--transform-file', help="Transform file to read",
              metavar='TRANSFORMFILE',
              default=None,
              required=False)
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def transform(ctx: click.Context,
              transform_file):
    profile_info : ProfileUserContext = ctx.obj['usercontext']
    project_name, table_name = profile_info.projectname, profile_info.tablename
    if not project_name:
        raise HdxCliException("Error. No project name provided and no 'projectname' set in profile")
    hostname = profile_info.hostname
    org_id = profile_info.org_id
    list_projects_url = f'https://{hostname}/config/v1/orgs/{org_id}/projects/'
    token = profile_info.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    projects_list = rest_ops.list(list_projects_url,
                                  headers=headers)

    project_id = [p['uuid'] for p in projects_list if p['name'] == project_name][0]

    list_tables_url = f'https://{hostname}/config/v1/orgs/{org_id}/projects/{project_id}/tables'
    tables_list = rest_ops.list(list_tables_url,
                                  headers=headers)
    table_id = [t['uuid'] for t in tables_list if t['name'] == table_name][0]


    transforms_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/transforms/'
    transforms_url = f'https://{hostname}{transforms_path}'

    transforms_list = rest_ops.list(transforms_url,
                                    headers=headers)
    transform_id = None

    if not profile_info.transformname:
        try:
            transform_name = [t['name'] for t in transforms_list if t['settings']['is_default']][0]
            profile_info.transformname = transform_name
        except:
            pass
    else:
        try:
            transform_name = [t['name'] for t in transforms_list if t['name'] == profile_info.transformname][0]
            profile_info.transformname = transform_name
        except IndexError as e:
            raise TransformNotFoundException(f'Transform not found: {profile_info.transformname}') from e
    ctx.obj = {'resource_path':
               transforms_path,
               'usercontext': profile_info}


transform.add_command(command_create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
#transform.add_command(command_set_default)
