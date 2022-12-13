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

from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings


@click.group(help="Function-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def function(ctx: click.Context):
    profileinfo = ctx.parent.obj['usercontext']
    hostname = profileinfo.hostname
    project_name = profileinfo.projectname
    if not project_name:
        raise LogicException(f"No project parameter was provided and no project is set in profile '{profileinfo.profilename}'")
    org_id = profileinfo.org_id
    scheme = profileinfo.scheme
    list_projects_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/'
    auth_token: AuthInfo = profileinfo.auth
    headers = {'Authorization': f'{auth_token.token_type} {auth_token.token}',
               'Accept': 'application/json'}
    projects_list = rest_ops.list(list_projects_url,
                                  headers=headers)
    project_id = [p['uuid'] for p in projects_list if p['name'] == project_name]
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/{project_id[0]}/functions/',
               'usercontext': profileinfo}


@click.command(help='Create sql function.')
@click.option('--sql-from-file', '-f',
              help='Create the body of the sql from a json description as in the POST request in https://docs.hydrolix.io/docs/custom-functions.'
              """For example:
              '{
                "sql": "(x, k, b) -> k*x + b;",
                "name": "linear_equation"
              }'"""
              ". 'name' will be replaced by FUNCTION_NAME",
              default=None)
@click.option('--inline-sql', '-s',
              help="Use inline sql in the command-line",
              default=None)
@click.argument('function_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           function_name: str,
           sql_from_file,
           inline_sql):
    if inline_sql and sql_from_file:
        raise LogicException('Only one of the options --inline-sql and --sql-from-file can be used.')
    if not inline_sql and not sql_from_file:
        raise LogicException('You need at least one of --inline-sql or --sql-from-file to create a function.')

    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    body = {}
    if sql_from_file:
        with open(sql_from_file, 'r', encoding='utf-8') as input_body:
            body = json.load(input_body)
            body['name'] = f'{function_name}'
    elif inline_sql:
        body['name'] = f'{function_name}'
        body['sql'] = inline_sql
    rest_ops.create(url, body=body, headers=headers)
    print(f'Created function {function_name}.')


function.add_command(create)
function.add_command(command_delete)
function.add_command(command_list)
function.add_command(command_show)
function.add_command(command_settings)
