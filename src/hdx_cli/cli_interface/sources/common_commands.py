import click

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.exceptions import HdxCliException, LogicException

from ..common.undecorated_click_commands import basic_create_with_body_from_string


def any_source_impl(ctx: click.Context, source_name):
    user_profile = ctx.parent.obj.get('usercontext')
    project_name, table_name = user_profile.projectname, user_profile.tablename
    if not project_name or not table_name:
        raise HdxCliException(f"No project/table parameters provided and "
                              f"no project/table set in profile '{user_profile.profilename}'")

    hostname = user_profile.hostname
    org_id = user_profile.org_id
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    list_projects_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/'
    token = user_profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    projects_list = rest_ops.list(list_projects_url,
                                  headers=headers,
                                  timeout=timeout)

    try:
        project_id = [p['uuid'] for p in projects_list if p['name'] == project_name][0]

        list_tables_url = f'{list_projects_url}{project_id}/tables/'
        tables_list = rest_ops.list(list_tables_url,
                                    headers=headers,
                                    timeout=timeout)
        table_id = [t['uuid'] for t in tables_list if t['name'] == table_name][0]

        sources_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/sources/'
    except IndexError as idx_err:
        raise LogicException('Cannot find resource.') from idx_err
    else:
        ctx.obj = {'resource_path': f'{sources_path}{source_name}/',
                   'usercontext': user_profile}


@click.command(help="Create source. 'source_filename' contains the settings. "
                    "name in settings will be replaced by 'source_name'")
@click.argument('source_filename')
@click.argument('source_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           source_filename: str,
           source_name: str):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    with open(source_filename, "r", encoding="utf-8") as file:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           source_name, file.read())
    print(f'Created source {source_name}')
