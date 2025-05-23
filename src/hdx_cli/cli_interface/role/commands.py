import json
import uuid

import click

from hdx_cli.cli_interface.role.utils import (
    Policy,
    Role,
    get_available_scope_type_list,
    get_role_data_from_standard_input,
    modify_role_data_from_standard_input,
)

from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from ...models import ProfileUserContext
from ..common.cached_operations import find_users
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import (
    basic_create,
    basic_show,
    basic_update,
    generic_basic_list,
)

logger = get_logger()


@click.group(help="Role-related operations")
@click.option(
    "--role",
    "role_name",
    metavar="ROLE_NAME",
    default=None,
    help="Perform operation on the passed role.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def role(ctx: click.Context, role_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, rolename=role_name)
    ctx.obj = {"resource_path": "/config/v1/roles/", "usercontext": user_profile}


def validate_uuid(ctx, param, value):
    if value is None:
        return None
    try:
        uuid_obj = uuid.UUID(value, version=4)
        return str(uuid_obj)
    except ValueError as exc:
        raise click.BadParameter(f"'{value}' is not a valid UUID.") from exc


def validate_scope_type(ctx, param, value):
    if value is None:
        return None

    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    available_scope_types = get_available_scope_type_list(profile, resource_path)

    if value not in available_scope_types:
        raise click.BadParameter(
            f"Invalid scope type '{value}'. Available options are: {', '.join(available_scope_types)}"
        )
    return value


@click.command(
    help="Create a new role. You can create a role by providing command-line "
    "options or interactively.\n\n"
    "Command-line options: "
    "Use flags like `-t`, `-i`, and `-p` to specify the scope type, "
    "scope ID (UUID), and permissions respectively.\n\n"
    "Interactive mode: "
    "If no options are provided, the HDXCLI will prompt you with questions "
    "to configure the new role."
)
@click.argument("role_name", metavar="ROLENAME", required=True)
@click.option(
    "--scope-type",
    "-t",
    "scope_type",
    metavar="SCOPE_TYPE",
    required=False,
    default=None,
    help="Type of scope for the role.",
    callback=validate_scope_type,
)
@click.option(
    "--scope-id",
    "-i",
    "scope_id",
    metavar="SCOPE_ID",
    type=str,
    required=False,
    default=None,
    help="Identifier for the scope (UUID).",
    callback=validate_uuid,
)
@click.option(
    "--permission",
    "-p",
    "permissions",
    required=False,
    multiple=True,
    default=None,
    help="Specify permissions for the new role (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context, role_name: str, scope_type: str, scope_id: str, permissions: list[str]
):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    if not scope_type and not scope_id and not permissions:
        # Interactive way
        role_obj = get_role_data_from_standard_input(profile, resource_path, role_name)
    elif permissions and ((scope_type and scope_id) or (not scope_type and not scope_id)):
        # Command-line way (making sure the necessary data is provided)
        policy_obj = Policy(scope_type=scope_type, scope_id=scope_id, permissions=list(permissions))
        role_obj = Role(name=role_name, policies=[policy_obj])
    else:
        # Handle all unexpected cases
        raise click.BadParameter(
            "Please provide either command-line options or "
            "enter interactive mode to create the role."
        )

    if role_obj:
        basic_create(
            profile,
            resource_path,
            role_obj.name,
            body=role_obj.model_dump(by_alias=True, exclude_none=True),
        )
        logger.info(f"Created role {role_obj.name}")
    else:
        logger.info("Role creation was cancelled")


@click.command(help="Modify an existing role.")
@click.argument("role_name", metavar="ROLE_NAME", required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def edit(ctx: click.Context, role_name: str):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    json_data = json.loads(basic_show(profile, resource_path, role_name))
    role_obj = Role(**json_data)
    role_to_update = modify_role_data_from_standard_input(profile, resource_path, role_obj)

    if not role_to_update:
        logger.info("Update was cancelled")
        return

    basic_update(
        profile,
        resource_path,
        resource_name=role_name,
        body=role_to_update.model_dump(by_alias=True, exclude_none=True),
    )
    logger.info(f"Updated role {role_name}")


@click.command(name="add-user", help="Add users to a role.")
@click.argument("role_name", metavar="ROLE_NAME")
@click.option(
    "-u",
    "--user",
    "users",
    multiple=True,
    default=None,
    required=True,
    help="Specify users to add to a role (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def add(ctx: click.Context, role_name: str, users):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _manage_users_from_role(profile, resource_path, role_name, users, action="add")
    logger.info(f"Added user(s) to {role_name} role")


@click.command(name="remove-user", help="Remove users from a role.")
@click.argument("role_name", metavar="ROLE_NAME")
@click.option(
    "-u",
    "--user",
    "users",
    multiple=True,
    default=None,
    required=True,
    help="Specify users to remove from a role (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove(ctx: click.Context, role_name: str, users):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _manage_users_from_role(profile, resource_path, role_name, users, action="remove")
    logger.info(f"Removed user(s) from {role_name} role")


@click.group(help="Permission-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def permission(ctx: click.Context):
    user_profile = ctx.parent.obj["usercontext"]
    ctx.obj = {"resource_path": "/config/v1/roles/permissions", "usercontext": user_profile}


@click.command(help="List permissions.", name="list")
@click.option(
    "--scope-type",
    "-t",
    "scope_type",
    metavar="SCOPE_TYPE",
    required=False,
    default=None,
    help="Filter the permissions by a specific scope type.",
)
@click.option("--page", "-p", type=int, default=1, help="Page number.")
@click.option("--page-size", "-s", type=int, default=None, help="Number of items per page.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, scope_type: str, page: int, page_size: int):
    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    _permission_list(profile, resource_path, scope_type, page, page_size)


def _permission_list(
    profile: ProfileUserContext,
    resource_path: str,
    scope_type: str,
    page: int,
    page_size: int,
):
    response = generic_basic_list(profile, resource_path, page=page, page_size=page_size)

    count, current_count, current, num_pages = None, None, None, None
    if "results" in response:
        permissions_role = response.get("results", [])
        count = response.get("count", 0)
        current_count = len(permissions_role)
        current = response.get("current", 0)
        num_pages = response.get("num_pages", 0)
    else:
        # If not paginated, assume the response is the list of resources.
        permissions_role = response

    logged = False
    for resource in permissions_role:
        current_scope = resource.get("scope_type")
        if scope_type and current_scope != scope_type:
            continue

        logged = True
        logger.info(f"Scope type: {current_scope}")
        for perm in resource.get("permissions", []):
            logger.info(f"  {perm}")

    if logged and count not in (None, 0):
        logger.info(f"Listed {current_count} of {count} permissions [page {current}/{num_pages}]")


def _get_user_uuids_by_emails(profile: ProfileUserContext, user_emails: list[str]) -> list[str]:
    users_uuid = []
    remaining_emails = set(user_emails)
    users_list = find_users(profile)

    for user in users_list:
        email = user.get("email")
        if email in remaining_emails:
            users_uuid.append(user["uuid"])
            remaining_emails.remove(email)

        if not remaining_emails:
            break  # Found all emails

    if remaining_emails:
        missing_emails = ", ".join(remaining_emails)
        raise ResourceNotFoundException(f"Cannot find users for emails: {missing_emails}.")

    return users_uuid


def _manage_users_from_role(
    profile: ProfileUserContext,
    resource_path: str,
    role_name: str,
    user_emails: list[str],
    action: str,
):
    role_json = json.loads(basic_show(profile, resource_path, role_name))
    role_id = role_json.get("id")
    if not role_id:
        raise LogicException(f"There was an error with the role {role_name}.")

    users_uuid = _get_user_uuids_by_emails(profile, user_emails)
    if len(user_emails) != len(users_uuid):
        raise ResourceNotFoundException("Cannot find some user.")

    # Creating body with each uuid
    user_body_list = [{"uuid": user_uuid} for user_uuid in users_uuid]
    body = {"users": user_body_list}

    action_resource_path = f"{resource_path}{role_id}/{action}_user/"
    basic_create(profile, action_resource_path, body=body)


role.add_command(command_list)
role.add_command(create)
role.add_command(edit)
role.add_command(add)
role.add_command(remove)
role.add_command(command_delete)
role.add_command(command_show)
role.add_command(permission)
permission.add_command(list_)
