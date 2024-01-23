import json
import uuid
import click

from ...library_api.common.exceptions import (LogicException,
                                              InvalidRoleException,
                                              ResourceNotFoundException)
from ...library_api.common.role_validator import (Role, Policy,
                                                  get_role_data_from_standard_input,
                                                  modify_role_data_from_standard_input,
                                                  update_role_request)
from ...library_api.userdata.token import AuthInfo
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.context import ProfileUserContext
from ..common.undecorated_click_commands import basic_create_with_body_from_string, basic_show
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)


@click.group(help="Role-related operations")
@click.option('--role', 'role_name', help='Perform operation on the passed role.',
              metavar='ROLE_NAME', default=None)
@click.pass_context
def role(ctx: click.Context,
         role_name):
    user_profile = ctx.parent.obj['usercontext']
    ctx.obj = {'resource_path': '/config/v1/roles/',
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      rolename=role_name)


AVAILABLE_SCOPE_TYPE = ('user', 'pool', 'role', 'invite', 'org', 'project',
                        'alterjob', 'batchjob', 'catalog', 'hdxstorage',
                        'dictionary', 'dictionaryfile', 'function', 'table',
                        'kafkasource', 'siemsource', 'kinesissource',
                        'summarysource', 'transform', 'view')


def validate_uuid(ctx, param, value):
    if value is None:
        return None
    try:
        uuid_obj = uuid.UUID(value, version=4)
        return str(uuid_obj)
    except ValueError:
        raise click.BadParameter(f"'{value}' is not a valid UUID.")


@click.command(help='Create a new role. You can create a role by providing command-line '
                    'options or interactively.\n\n'
                    'Command-line options: '
                    'Use flags like `-n`, `-t`, `-i`, and `-p` to specify the role name, '
                    'scope type, scope ID (UUID), and permissions respectively.\n\n'
                    'Interactive mode: '
                    'If no options are provided, the HDXCLI will prompt you with questions '
                    'to configure the new role. '
                    'Simply press Enter to start the interactive configuration process.')
@click.option('--name', '-n', 'role_name',
              type=str,
              help='Name of the role.',
              metavar='ROLE_NAME',
              required=False,
              default=None)
@click.option('--scope-type', '-t', 'scope_type',
              type=click.Choice(AVAILABLE_SCOPE_TYPE),
              help='Type of scope for the role.',
              metavar='SCOPE_TYPE',
              required=False,
              default=None)
@click.option('--scope-id', '-i', 'scope_id',
              type=str,
              help='Identifier for the scope (UUID).',
              metavar='SCOPE_ID',
              callback=validate_uuid,
              required=False,
              default=None)
@click.option('--permission', '-p', 'permissions',
              help='Specify permissions for the new role (can be used multiple times).',
              required=False,
              multiple=True,
              default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           role_name: str,
           scope_type: str,
           scope_id,
           permissions):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')

    role_obj = None
    if not role_name and not scope_type and not scope_id and not permissions:
        # Interactive way
        role_obj = get_role_data_from_standard_input(profile, resource_path)

    elif (role_name and permissions and
          ((scope_type and scope_id) or (not scope_type and not scope_id))):
        # Command-line way (making sure the necessary data is provided)
        policy_obj = Policy(scope_type=scope_type,
                            scope_id=scope_id,
                            permissions=list(permissions))
        role_obj = Role(name=role_name,
                        policies=[policy_obj])

    else:
        # Handle all unexpected cases
        raise click.BadParameter('Please provide either command-line options or '
                                 'enter interactive mode to create the role.')

    if role_obj:
        basic_create_with_body_from_string(profile,
                                           resource_path,
                                           role_obj.name,
                                           role_obj.model_dump_json(by_alias=True, exclude_none=True))
        print(f"Created role {role_obj.name}")
    else:
        print("Role creation was cancelled")


@click.command(help='Modify an existing role.')
@click.argument('role_name', metavar='ROLE_NAME')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def edit(ctx: click.Context,
         role_name: str):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    json_data = json.loads(basic_show(profile, resource_path, role_name))
    role_obj = Role(**json_data)
    role_to_update = modify_role_data_from_standard_input(profile, resource_path, role_obj)

    resource_path = f"{resource_path}{role_to_update.id}/"

    if role_to_update:
        update_role_request(profile,
                            resource_path,
                            role_to_update.model_dump(by_alias=True, exclude_none=True))
    else:
        print("Update was cancelled")


@click.command(name='add-user', help='Add users to a role.')
@click.argument('role_name', metavar='ROLE_NAME')
@click.option('-u', '--user', 'users',
              help='Specify users to add to a role (can be used multiple times).',
              multiple=True, default=None, required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def add(ctx: click.Context,
        role_name,
        users):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    _manage_users_from_role(profile,
                            resource_path,
                            role_name,
                            users,
                            action='add')
    print(f'Users added to {role_name} role')


@click.command(name='remove-user', help='Remove users from a role.')
@click.argument('role_name', metavar='ROLE_NAME')
@click.option('-u', '--user', 'users',
              help='Specify users to remove from a role (can be used multiple times).',
              multiple=True, default=None, required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove(ctx: click.Context,
           role_name,
           users):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    _manage_users_from_role(profile,
                            resource_path,
                            role_name,
                            users,
                            action='remove')
    print(f'Users removed from {role_name} role')


@click.group(help="Permission-related operations.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def permission(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    ctx.obj = {'resource_path': '/config/v1/roles/permissions',
               'usercontext': user_profile}


@click.command(help='List permissions.', name='list')
@click.option('--scope-type', '-t', 'scope_type',
              type=click.Choice(AVAILABLE_SCOPE_TYPE),
              help='Filter the permissions by a specific scope type.',
              metavar='SCOPE_TYPE',
              required=False,
              default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context,
          scope_type: str):
    resource_path = ctx.parent.obj.get('resource_path')
    profile = ctx.parent.obj.get('usercontext')
    _basic_list(profile, resource_path, scope_type)


def _basic_list(profile, resource_path, scope_type=None):
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
    if scope_type:
        for resource in resources:
            if resource.get("scope_type") == scope_type:
                for perm in resource.get("permissions"):
                    print(f'{perm}')
    else:
        for resource in resources:
            print(f'Scope type: {resource.get("scope_type")}')
            for perm in resource.get("permissions"):
                print(f'  {perm}')


def _validate_role(profile, roles: tuple) -> list:
    """
    Checks if each name in the 'roles' list exists in the created Hydrolix roles.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f'{scheme}://{hostname}/config/v1/roles/'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url,
                              headers=headers,
                              timeout=timeout)

    roles = list(set(roles))
    existing_roles = [item['name'] for item in resources]
    valid_roles, invalid_roles = [], []
    for role_name in roles:
        if role_name not in existing_roles:
            invalid_roles.append(role_name)
        else:
            valid_roles.append(role_name)

    if invalid_roles:
        raise InvalidRoleException(
            f"Invalid role(s) {', '.join(invalid_roles)}.")

    return valid_roles


def _get_users_uuid(profile, users):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f'{scheme}://{hostname}/config/v1/users/'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url,
                              headers=headers,
                              timeout=timeout)
    users_uuid = []
    for user in resources:
        if user['email'] in users:
            users_uuid.append(user['uuid'])

    if not users_uuid:
        raise ResourceNotFoundException('Cannot find users.')
    return users_uuid


def _manage_users_from_role(profile, resource_path,
                            role_name, users, action: str):
    role_json = json.loads(basic_show(profile,
                                      resource_path,
                                      role_name))
    role_id = role_json.get('id')
    if not role_id:
        raise LogicException(f'There was an error with the role {role_name}.')

    users_uuid = _get_users_uuid(profile, users)
    if len(users) != len(users_uuid):
        raise ResourceNotFoundException('Cannot find some user.')

    # Creating body with each uuid
    user_body_list = [{"uuid": user_uuid} for user_uuid in users_uuid]
    body = {"users": user_body_list}

    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    # 'action' variable must be 'add' or 'remove'
    add_users_url = f'{scheme}://{hostname}{resource_path}{role_id}/{action}_user/'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}

    rest_ops.create(add_users_url, headers=headers,
                    timeout=timeout,
                    body=body)


role.add_command(command_list)
role.add_command(create)
role.add_command(edit)
role.add_command(add)
role.add_command(remove)
role.add_command(command_delete)
role.add_command(command_show)
role.add_command(permission)
permission.add_command(list_)
