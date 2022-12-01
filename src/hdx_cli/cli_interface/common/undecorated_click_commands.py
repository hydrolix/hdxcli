from typing import Optional

from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.exceptions import LogicException, HdxCliException

import json
import click

from ...library_api.common import rest_operations as rest_ops


def basic_create(profile,
                 resource_path,
                 resource_name: str,
                 body_from_file: Optional[str],
                 body_from_file_type='json',
                 # sql
                 ):
    hostname = profile.hostname
    url = f'https://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    body = {}
    body_stream = None
    if body_from_file:
        # This parameter is for dictionaries
        if body_from_file_type == 'json':
            with open(body_from_file, 'r', encoding='utf-8') as input_body:
                body = json.load(input_body)
                body['name'] = f'{resource_name}'
        else:
            body_stream = open(body_from_file, 'rb')  # pylint:disable=consider-using-with
    # elif sql:
    #     body['name'] = f'{resource_name}'
    #     body['sql'] = sql
    else:
        body = {'name': f'{resource_name}',
                'description': 'Created with hdxcli tool'}
    if body_from_file_type == 'json':
        rest_ops.create(url, body=body, headers=headers)
    elif body_from_file and body_stream:
        rest_ops.create_file(url, headers=headers, file_stream=body_stream,
                             remote_filename=resource_name)
        if body_stream:
            body_stream.close()
    else:
        rest_ops.create(url, body=body, headers=headers)


def basic_create_with_body_from_string(profile,
                 resource_path,
                 resource_name: str,
                 body_from_string: Optional[str]
                 # body_from_file_type='json',
                 # sql
                 ):
    hostname = profile.hostname
    url = f'https://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    body = json.loads(body_from_string)
    body['name'] = f'{resource_name}'
    rest_ops.create(url, body=body, headers=headers)



def basic_show(profile, resource_path, resource_name):
    hostname = profile.hostname
    list_url = f'https://{hostname}{resource_path}'
    auth_info : AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)

    for resource in resources:
        if resource['name'] == resource_name:
            return json.dumps(resource)


def basic_transform(ctx: click.Context):
    profile_info: ProfileUserContext = ctx.obj['usercontext']
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

    try:
        project_id = [p['uuid'] for p in projects_list if p['name'] == project_name][0]

        list_tables_url = f'https://{hostname}/config/v1/orgs/{org_id}/projects/{project_id}/tables'
        tables_list = rest_ops.list(list_tables_url,
                                    headers=headers)
        table_id = [t['uuid'] for t in tables_list if t['name'] == table_name][0]

        transforms_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/transforms/'
        transforms_url = f'https://{hostname}{transforms_path}'

        transforms_list = rest_ops.list(transforms_url,
                                        headers=headers)
    except IndexError as idx_err:
        raise LogicException('Cannot find resource.') from idx_err

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
        except IndexError as ex:
            raise TransformNotFoundException(f'Transform not found: {profile_info.transformname}') from ex
    ctx.obj = {'resource_path':
               transforms_path,
               'usercontext': profile_info}
