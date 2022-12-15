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
from .undecorated_click_commands import (basic_create,
                                         basic_delete,
                                         basic_list,
                                         basic_show)


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
# @click.option('--sql', '-s',
#               help="Create will use as 'sql' field the contents of the sql string",
#               default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           resource_name: str,
           body_from_file,
           body_from_file_type):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    basic_create(user_profile, resource_path,
                 resource_name, body_from_file, body_from_file_type)
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
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str,
           disable_confirmation_prompt):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    if basic_delete(profile, resource_path, resource_name):
        print(f'Deleted {resource_name}')
    else:
        print(f'Could not delete {resource_name}. Not found.')


@click.command(help='List resources.', name='list')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    basic_list(profile, resource_path)


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
    elif plural == 'kinesis':
        return 'kinesis', 'kinesis'
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular


@click.command(help='Show resource. If not resource_name is provided, it will show the default if there is one.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context):
    profile = ctx.parent.obj['usercontext']
    _, resource_kind = _heuristically_get_resource_kind(ctx.parent.obj['resource_path'])
    resource_name = getattr(profile, resource_kind + 'name')

    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    print(basic_show(profile, resource_path,
                     resource_name))
