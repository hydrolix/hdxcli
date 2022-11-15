from functools import partial
from typing import Tuple
import json

import click

from ...library_api.common.exceptions import (HdxCliException,
                                              ResourceNotFoundException)
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import (report_error_and_exit,
                                               dynamic_confirmation_prompt)
from .cached_operations import *

@click.command(help='Create resource.')
@click.option('--body-from-file', '-f',
              help='Create will use as body for request the file contents.'
              "'name' key from the body will be replaced by the given 'resource_name'.",
              default=None)
@click.option('--body-from-file', '-f',
              help='Create will use as body the file contents.',
              default=None)
@click.option('--body-from-file-type', '-t',
              type=click.Choice(('json', 'verbatim')),
              help='How to interpret the body from option. ',
              default='json')
@click.argument('resource_name')
@click.option('--sql', '-s',
              help="Create will use as 'sql' field the contents of the sql string",
                default=None)
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def create(ctx: click.Context,
           resource_name: str,
           body_from_file,
           body_from_file_type,
           sql):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    url = f'https://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    body = {}
    body_stream = None
    if body_from_file:
        if body_from_file_type == 'json':
            with open(body_from_file, 'r', encoding='utf-8') as input_body:  
                body = json.load(input_body)
                body['name'] = f'{resource_name}'
        else:
            body_stream = open(body_from_file, 'rb') # pylint:disable=consider-using-with
    elif sql:
        body['name'] = f'{resource_name}'
        body['sql'] = sql
    else:
        body = {'name': f'{resource_name}',
                'description': 'Created with hdx-cli tool'}
    
    if body_from_file_type == 'json':
        rest_ops.create(url, body=body, headers=headers)
    else:
        rest_ops.create_file(url, headers=headers, file_stream=body_stream,
                             remote_filename=resource_name)
        body_stream.close()
    print(f'Created {resource_name}.')


_confirmation_prompt = partial(dynamic_confirmation_prompt,
                        prompt="Please type 'delete this resource' to delete: ",
                        confirmation_message='delete this resource',
                        fail_message='Incorrect prompt input: resource was not deleted')
@click.command(help='Delete resource.')
@click.option('--disable-confirmation-prompt', 
is_flag=True,
help='Suppress confirmation to delete resource.', show_default=True, default=False)
@click.argument('resource_name')
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def delete(ctx: click.Context, resource_name: str,
           disable_confirmation_prompt):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    list_url = f'https://{hostname}{resource_path}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    url = None
    for a_resource in resources:
        if a_resource['name'] == resource_name:
            url = a_resource['url']
            break
    if not url:
        print(f'Could not delete {ctx.parent.command.name} {resource_name}. Not found.')
    else:
        rest_ops.delete(url, headers=headers)
        print(f'Deleted {resource_name}')


@click.command(help='List resources.')
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def list(ctx: click.Context):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    list_url = f'https://{hostname}{resource_path}'
    auth_info : AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    for resource in resources:
        print(resource['name'], end='')
        if (settings := resource.get('settings')) and settings.get('is_default'):
            print(' (default)', end='')
        print()


def _heuristically_get_resource_kind(resource_path) -> Tuple[str, str]:
    """Returns plural and singular names for resource kind given a resource path.
       If it is a nested resource
    For example:

          - /config/.../tables/ -> ('tables', 'table')
          - /config/.../projects/ -> ('projects', 'project')
          - /config/.../jobs/batch/ -> ('batch', 'batch')
    """
    split_path = resource_path.split("/")
    plural = split_path[-2]
    if plural == 'dictionaries':
        return 'dictionaries', 'dictionary'
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular


@click.command(help='Show resource. If not resource_name is provided, it will show the default if there is one.')
@click.option('--show-all', '-s',
              is_flag=True,
              required=False,
              default=False)
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def show(ctx: click.Context,
        # TODO: all commands should support an optional resource name that overrides the prefix-style ---someresourcetype-name
         # It seems there are good use cases for it...
         # resource_name,
         show_all=False):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    list_url = f'https://{hostname}{resource_path}'
    auth_info : AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}

    resources = rest_ops.list(list_url, headers=headers)
    if show_all:
        for resource in resources:
            print(json.dumps(resource))
    else:
        _, resource_kind = _heuristically_get_resource_kind(resource_path)
        resource_name = getattr(profile, f'{resource_kind}name')
        for resource in resources:
            if resource['name'] == resource_name:
                print(json.dumps(resource))
                break
        else:
            raise ResourceNotFoundException(f'{resource_name} not found')
