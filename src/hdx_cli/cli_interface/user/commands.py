from functools import partial
import re
import json
import click

from ...library_api.common.exceptions import LogicException, HttpException
from ...library_api.userdata.token import AuthInfo
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import (report_error_and_exit,
                                               dynamic_confirmation_prompt)
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger
from ..common.undecorated_click_commands import basic_delete, basic_show, basic_create_from_dict_body

logger = get_logger()


@click.group(help='User-related operations')
@click.option('--user', 'user_email', metavar='USER_EMAIL', default=None,
              help='Perform operation on the passed user.')
@click.pass_context
def user(ctx: click.Context,
         user_email):
    user_profile = ctx.parent.obj['usercontext']
    ctx.obj = {'resource_path': '/config/v1/users/',
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      useremail=user_email)


@click.command(help='List users.', name='list')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    resource_path = ctx.parent.obj.get('resource_path')
    profile = ctx.parent.obj.get('usercontext')
    _basic_list(profile, resource_path)


@click.command(help='Show user. If not resource_name is provided, it will show the default if there is one.')
@click.option('-i', '--indent', is_flag=True, default=False,
              help='Number of spaces for indentation in the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    if not (resource_name := getattr(profile, 'useremail')):
        raise LogicException('No default user found in profile.')

    logger.info(basic_show(profile, resource_path, resource_name,
                           indent=indent, filter_field='email'))


@click.command(help='Send invitation to a new user.')
@click.argument('email', metavar='USER_EMAIL')
@click.option('-r', '--role', 'roles', multiple=True, default=None, required=True,
              help='Specify the role for the new user (can be used multiple times).')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def invite(ctx: click.Context,
           email,
           roles):
    resource_path = '/config/v1/invite/'
    profile = ctx.parent.obj.get('usercontext')

    org_id = profile.org_id
    body = {
        'email': email,
        'org': org_id,
        'roles': roles
    }
    basic_create_from_dict_body(profile, resource_path, body)
    logger.info(f'Invitation sent to {email}')


_confirmation_prompt = partial(dynamic_confirmation_prompt,
                               prompt="Please type 'delete this resource' to delete: ",
                               confirmation_message='delete this resource',
                               fail_message='Incorrect prompt input: resource was not deleted')


@click.command(help='Delete user.')
@click.option('--disable-confirmation-prompt', is_flag=True, show_default=True, default=False,
              help='Suppress confirmation to delete resource.')
@click.argument('email', metavar='USER_EMAIL')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, email: str,
           disable_confirmation_prompt: bool):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get('resource_path')
    user_profile = ctx.parent.obj.get('usercontext')
    if basic_delete(user_profile, resource_path, email, filter_field='email'):
        logger.info(f'Deleted {email}')
    else:
        logger.info(f'Could not delete {email}. Not found')


@click.command(name='assign-role', help='Assign roles to a user.')
@click.argument('email', metavar='USER_EMAIL')
@click.option('-r', '--role', 'roles', multiple=True, default=None, required=True,
              help='Specify roles to assign to a user (can be used multiple times).')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def assign(ctx: click.Context,
           email,
           roles):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    user_uuid = json.loads(basic_show(profile, resource_path,
                                      email,
                                      filter_field='email')).get('uuid')
    if not user_uuid:
        raise LogicException(f'There was an error with the user {email}.')

    resource_path = f'{resource_path}{user_uuid}/add_roles/'
    body = {
        'roles': roles
    }
    basic_create_from_dict_body(profile, resource_path, body)
    logger.info(f'Added role(s) to {email}')


@click.command(name='remove-role', help='Remove roles from a user.')
@click.argument('email', metavar='USER_EMAIL')
@click.option('-r', '--role', 'roles', multiple=True, default=None, required=True,
              help='Specify roles to remove from a user (can be used multiple times).')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove(ctx: click.Context,
           email,
           roles):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    user_json = json.loads(basic_show(profile, resource_path,
                                      email,
                                      filter_field='email'))
    user_uuid = user_json.get('uuid')
    user_roles = user_json.get('roles')
    if not user_uuid or not user_roles:
        raise LogicException(f'There was an error with the user {email}.')

    set_roles_to_remove = set(roles)
    set_user_roles = set(user_roles)
    if not set_roles_to_remove.issubset(set_user_roles):
        raise LogicException(f'User {email} lacks {list(set_roles_to_remove - set_user_roles)} '
                             f'role(s) for removal.')

    resource_path = f'{resource_path}{user_uuid}/remove_roles/'
    body = {
        'roles': list(roles)
    }
    basic_create_from_dict_body(profile, resource_path, body)
    logger.info(f'Removed role(s) from {email}')


def _basic_list(profile, resource_path):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url,
                              headers=headers,
                              timeout=timeout)
    for resource in resources:
        roles_name = resource.get("roles")
        logger.info(f'Email: {resource["email"]} | Roles: {(", ".join(roles_name))}')


user.add_command(list_)
user.add_command(invite)
user.add_command(assign)
user.add_command(remove)
user.add_command(delete)
user.add_command(show)
