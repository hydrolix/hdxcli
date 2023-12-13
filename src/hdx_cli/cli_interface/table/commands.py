"""Commands relative to project handling  operations"""
import json
import click
import requests

from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import HdxCliException, LogicException
from ...library_api.common.context import ProfileUserContext
from ...library_api.userdata.token import AuthInfo

from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show,
                                      activity as command_activity,
                                      stats as command_stats)

from ..common.misc_operations import settings as command_settings
from ..common.undecorated_click_commands import (basic_create,
                                                 basic_create_with_body_from_string)


@click.group(help="Table-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def table(ctx: click.Context,
          project_name,
          table_name):
    user_profile = ctx.parent.obj.get('usercontext')
    hostname = user_profile.hostname
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name)

    project = user_profile.projectname
    if not project:
        raise LogicException(f"No project parameter provided and "
                             f"no project is set in profile '{user_profile.profilename}'")
    org_id = user_profile.org_id
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    list_projects_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/'
    auth_token: AuthInfo = user_profile.auth
    headers = {'Authorization': f'{auth_token.token_type} {auth_token.token}',
               'Accept': 'application/json'}
    try:
        projects_list = rest_ops.list(list_projects_url,
                                      headers=headers,
                                      timeout=timeout)
        project_id = [p['uuid'] for p in projects_list if p['name'] == project]
        ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/{project_id[0]}/tables/',
                   'usercontext': user_profile}
    except IndexError as idx_err:
        raise LogicException(f'Cannot find project: {project}') from idx_err


@click.command(help='Create table.')
@click.argument('table_name')
@click.option('--type', '-t', 'table_type',
              type=click.Choice(('summary', 'turbine')),
              help='Create a raw (regular) table or an aggregation (summary) table',
              metavar='TYPE',
              required=False,
              default='turbine')
@click.option('--sql-query', '-s',
              type=str,
              help='SQL query to use (for summary tables only)',
              metavar='SQL_QUERY',
              required=False,
              default=None)
@click.option('--sql-query-file', '-f',
              type=str,
              help='File path to SQL query to use (for summary tables only)',
              metavar='SQL_QUERY_FILE',
              required=False,
              default=None)
@click.option('--ingestion-type', '-i',
              type=click.Choice(('stream', 'kafka', 'kinesis')),
              help='Ingest type (for summary tables only). '
                   'Default: stream (stream, kafka, kinesis)',
              metavar='INGESTION_TYPE',
              required=False,
              default='stream')
@click.option('--source-name', '-o',
              type=str,
              help='Source name if ingest type is kafka or kinesis '
                   '(for summary tables only)',
              metavar='SOURCE_NAME',
              required=False,
              default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           table_name: str,
           table_type: str,
           sql_query,
           sql_query_file,
           ingestion_type,
           source_name):
    if table_type == 'summary' and not (
            (sql_query and not sql_query_file) or (sql_query_file and not sql_query)):
        raise HdxCliException('When creating a summary table, either SQL query or SQL query file must be provided')

    if table_type == 'summary' and ingestion_type != 'stream' and not source_name:
        raise HdxCliException('If the ingestion type is kafka or kinesis, you must specify the source name '
                              'passing --source-name or -o')

    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    if table_type == 'summary':
        _basic_summary_create(user_profile, resource_path,
                              table_name, table_type, sql_query,
                              sql_query_file, ingestion_type,
                              source_name)
    else:
        basic_create(user_profile, resource_path,
                     table_name, None, None)
    print(f'Created table {table_name}')


def _basic_summary_create(user_profile,
                          resource_path,
                          table_name,
                          table_type,
                          sql_query,
                          sql_query_file,
                          ingestion_type,
                          source_name):
    if sql_query_file:
        try:
            with open(sql_query_file, 'r') as file:
                sql_query = file.read()
        except Exception as e:
            raise HdxCliException(f"reading SQL query from file '{sql_query_file}'") from e

    body = {
        'settings': {
            'summary': {
                'enabled': True,
                'sql': sql_query
            }
        },
        'type': table_type
    }
    if ingestion_type != 'stream':
        body['settings']['summary'][ingestion_type] = {'parent_source': source_name}

    basic_create_with_body_from_string(user_profile, resource_path, table_name, json.dumps(body))


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
        print(f'Error truncating table {table_name}')
        return
    print(f'Truncated table {table_name}')


table.add_command(create)
table.add_command(command_delete)
table.add_command(command_list)
table.add_command(command_show)
table.add_command(command_settings)
table.add_command(command_truncate, name='truncate')
table.add_command(command_activity)
table.add_command(command_stats)
