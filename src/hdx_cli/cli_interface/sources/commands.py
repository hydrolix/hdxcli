"""Commands relative to sources handling  operations"""
import click

from ...library_api.common.context import ProfileUserContext

from ...library_api.common import auth as auth_api
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import HdxCliException, LogicException

from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from .kafkakinesis import (kafka as command_kafka,
                           kinesis as command_kinesis)
from .summary import summary as command_summary


@click.group(help="Sources-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def sources(ctx: click.Context):
    profile_info : ProfileUserContext = ctx.obj['usercontext']
    project_name, table_name = profile_info.projectname, profile_info.tablename
    if not project_name:
        raise HdxCliException("Error. No project name provided and no 'projectname' set in profile")
    hostname = profile_info.hostname
    org_id = profile_info.org_id
    scheme = profile_info.scheme
    list_projects_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/'
    token = profile_info.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    projects_list = rest_ops.list(list_projects_url,
                                  headers=headers)

    try:
        project_id = [p['uuid'] for p in projects_list if p['name'] == project_name][0]

        list_tables_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/{project_id}/tables'
        tables_list = rest_ops.list(list_tables_url,
                                    headers=headers)
        table_id = [t['uuid'] for t in tables_list if t['name'] == table_name][0]
        sources_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/sources/'
    except IndexError as idx_err:
        raise LogicException('Cannot find resource.') from idx_err
    else:
        ctx.obj = {'resource_path': sources_path,
                   'usercontext': profile_info}


sources.add_command(command_kafka)
sources.add_command(command_kinesis)
sources.add_command(command_summary)
