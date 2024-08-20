"""Commands relative to tables handling operations"""
import click
import requests

from ..common.migration import migrate_a_table
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.generic_resource import access_resource
from ...library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger
from ...library_api.userdata.token import AuthInfo

from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show,
                                      activity as command_activity,
                                      stats as command_stats)

from ..common.misc_operations import settings as command_settings
from ..common.undecorated_click_commands import basic_create_from_dict_body
from ...library_api.utility.file_handling import load_json_settings_file, load_plain_file

logger = get_logger()


@click.group(help="Table-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def table(ctx: click.Context,
          project_name: str,
          table_name: str):
    user_profile = ctx.parent.obj.get('usercontext')
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name)
    project_name = user_profile.projectname
    if not project_name:
        raise LogicException(f"No project parameter provided and "
                             f"no project is set in profile '{user_profile.profilename}'")

    project_body = access_resource(user_profile, [('projects', project_name)])
    if not project_body:
        raise ResourceNotFoundException(f"Project '{project_name}' not found.")

    project_id = project_body.get('uuid')
    org_id = user_profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/',
               'usercontext': user_profile}


@click.command(help='Create table.')
@click.argument('table_name')
@click.option('--type', '-t', 'table_type', type=click.Choice(('turbine', 'summary')),
              required=False, default='turbine',
              help='Create a regular table or an aggregation table (summary). Default: turbine')
@click.option('--sql-query', '-s', type=str, required=False, default=None,
              help='SQL query to use (for summary tables only)')
@click.option('--sql-query-file', '-f', type=click.Path(), required=False, default=None,
              callback=load_plain_file,
              help='File path to SQL query to use (for summary tables only)')
@click.option('--settings-file', '-S', type=click.Path(), required=False, default=None,
              callback=load_json_settings_file,
              help='Path to a file containing settings for the table')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           table_name: str,
           table_type: str,
           sql_query: str,
           sql_query_file: str,
           settings_file: dict):
    if table_type == 'summary' and not (
            (sql_query and not sql_query_file) or (sql_query_file and not sql_query)):
        raise click.MissingParameter(
            'When creating a summary table, either SQL query or SQL query file must be provided.')

    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')

    body = {}
    if settings_file:
        body.update(settings_file)

    body.update({'name': table_name})

    if table_type == 'summary':
        summary_sql_query = sql_query_file if sql_query_file else sql_query
        body['type'] = 'summary'

        settings = body.get('settings', {})
        summary_settings = settings.get('summary', {})
        summary_settings['sql'] = summary_sql_query
        settings['summary'] = summary_settings
        body['settings'] = settings

    basic_create_from_dict_body(user_profile, resource_path, body)
    logger.info(f'Created table {table_name}')


def _basic_truncate(profile, resource_path, resource_name: str):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers, timeout=timeout)
    url = None
    for a_resource in resources:
        if a_resource['name'] == resource_name:
            if 'url' in a_resource:
                url = a_resource['url'].replace('https://', f'{scheme}://')
            else:
                url = f"{scheme}://{hostname}{resource_path}{a_resource['uuid']}"
            break
    if not url:
        return False
    url = f'{url}/truncate'
    result = requests.post(url,
                           headers=headers,
                           timeout=timeout)
    if result.status_code not in (200, 201):
        return False
    return True


@click.command(help='Truncate table.')
@click.argument('table_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def command_truncate(ctx: click.Context,
                     table_name):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    if not _basic_truncate(user_profile, resource_path, table_name):
        logger.info(f'Could not truncate table {table_name}')
        return
    logger.info(f'Truncated table {table_name}')


@click.command(help='Migrate a table.')
@click.argument('table_name', metavar='TABLE_NAME', required=True, default=None)
@click.option('-tp', '--target-profile', required=False, default=None)
@click.option('-h', '--target-cluster-hostname', required=False, default=None)
@click.option('-u', '--target-cluster-username', required=False, default=None)
@click.option('-p', '--target-cluster-password', required=False, default=None)
@click.option('-s', '--target-cluster-uri-scheme', required=False, default='https')
@click.option('-P', '--target-project-name', required=True, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context,
            table_name: str,
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
    migrate_a_table(user_profile,
                    table_name,
                    target_profile,
                    target_cluster_hostname,
                    target_cluster_username,
                    target_cluster_password,
                    target_cluster_uri_scheme,
                    target_project_name)
    logger.info(f'Migrated table {table_name}')


table.add_command(create)
table.add_command(command_delete)
table.add_command(command_list)
table.add_command(command_show)
table.add_command(command_settings)
table.add_command(command_truncate, name='truncate')
table.add_command(command_activity)
table.add_command(command_stats)
table.add_command(migrate)
