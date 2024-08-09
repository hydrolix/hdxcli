"""Commands relative to project handling  operations"""
import json
import click

from ..common.migration import migrate_a_function
from ...library_api.common.generic_resource import access_resource
from ...library_api.userdata.token import AuthInfo
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.context import ProfileUserContext
from ...library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common.logging import get_logger
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings

logger = get_logger()


@click.group(help="Function-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--function', 'function_name', help="Perform operation on the passed function.",
              metavar='FUNCTIONNAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def function(ctx: click.Context,
             project_name: str,
             function_name: str):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(
        user_profile,
        projectname=project_name,
        functionname=function_name
    )

    project_name = user_profile.projectname
    if not project_name:
        raise LogicException(f"No project parameter provided and "
                             f"no project set in profile '{user_profile.profilename}'")

    project_body = access_resource(user_profile, [('projects', project_name)])
    if not project_body:
        raise ResourceNotFoundException(f"Project '{project_name}' not found.")

    project_id = project_body.get('uuid')
    org_id = user_profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/{project_id}/functions/',
               'usercontext': user_profile}


@click.command(help='Create sql function.')
@click.option('--sql-from-file', '-f',
              help='Create the body of the sql from a json description as in the POST request in '
                   'https://docs.hydrolix.io/docs/custom-functions.'
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
           sql_from_file: str,
           inline_sql: str):
    if inline_sql and sql_from_file:
        raise LogicException(
            'Only one of the options --inline-sql and --sql-from-file can be used.')
    if not inline_sql and not sql_from_file:
        raise LogicException(
            'You need at least one of --inline-sql or --sql-from-file to create a function.')

    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
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
    rest_ops.create(url, body=body, headers=headers, timeout=timeout)
    logger.info(f'Created function {function_name}')


@click.command(help='Migrate a function.')
@click.argument('function_name', metavar='FUNCTION_NAME', required=True, default=None)
@click.option('-tp', '--target-profile', required=False, default=None)
@click.option('-h', '--target-cluster-hostname', required=False, default=None)
@click.option('-u', '--target-cluster-username', required=False, default=None)
@click.option('-p', '--target-cluster-password', required=False, default=None)
@click.option('-s', '--target-cluster-uri-scheme', required=False, default='https')
@click.option('-P', '--target-project-name', required=True, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context,
            function_name: str,
            target_profile,
            target_cluster_hostname,
            target_cluster_username,
            target_cluster_password,
            target_cluster_uri_scheme,
            target_project_name):
    if target_profile is None and not (target_cluster_hostname and target_cluster_username
                                       and target_cluster_password and target_cluster_uri_scheme):
        raise click.BadParameter('Either provide a --target-profile or all four target cluster options.')

    user_profile = ctx.parent.obj['usercontext']
    migrate_a_function(
        user_profile,
        function_name,
        target_profile,
        target_cluster_hostname,
        target_cluster_username,
        target_cluster_password,
        target_cluster_uri_scheme,
        target_project_name
    )
    logger.info(f'Migrated function {function_name}')


function.add_command(create)
function.add_command(command_delete)
function.add_command(command_list)
function.add_command(command_show)
function.add_command(command_settings)
function.add_command(migrate)
