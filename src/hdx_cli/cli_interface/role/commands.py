import json
import uuid

import click
from rich.console import Console
from rich.columns import Columns
from rich.table import Table

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.cached_operations import find_users, find_permissions
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create,
    basic_show,
    basic_update,
)
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from .utils import (
    get_available_scope_type_list,
    get_role_data_from_standard_input,
    Policy,
    Role,
    modify_role_data_from_standard_input,
)
from hdx_cli.library_api.common.exceptions import ResourceNotFoundException, LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
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
    """Commands to create, edit, and manage user roles and
    their permissions."""
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


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--scope-type",
    "-t",
    required=False,
    default=None,
    help="Type of scope for the role.",
    callback=validate_scope_type,
)
@click.option(
    "--scope-id",
    "-i",
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
@click.option(
    "--interactive",
    is_flag=True,
    help="Enter interactive mode to be guided through role creation.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    scope_type: str,
    scope_id: str,
    permissions: list[str],
    interactive: bool,
):
    """Create a new {resource}. This command supports two modes
    for creating a {resource}:

    \b
    1. *Command-Line*: Define a single policy by providing its details as options.
    2. *Interactive*: Use the `--interactive` flag for a guided setup.

    \b
    Examples:
      # Create a {resource} with a single global permission
      {full_command_prefix} create my_read_role --permission read_table

    \b
      # Create a {resource} with project-scoped permissions
      {full_command_prefix} create my_project_role --scope-type project --scope-id <uuid> --permission add_table

    \b
      # Start the interactive guide to create a {resource}
      {full_command_prefix} create my_interactive_role --interactive
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    role_obj = None
    if interactive:
        role_obj = get_role_data_from_standard_input(profile, resource_path, resource_name)
    elif permissions:
        if scope_type and not scope_id:
            raise click.BadOptionUsage("scope_id", "--scope-id is required when --scope-type is used.")
        if scope_id and not scope_type:
            raise click.BadOptionUsage("scope_type", "--scope-type is required when --scope-id is used.")
        policy_obj = Policy(scope_type=scope_type, scope_id=scope_id, permissions=list(permissions))
        role_obj = Role(name=resource_name, policies=[policy_obj])
    else:
        # Handle all unexpected cases
        raise click.BadParameter(
            "To create a role, you must either use the --interactive "
            "flag or provide at least one --permission."
        )

    if role_obj:
        basic_create(
            profile,
            resource_path,
            resource_name,
            body=role_obj.model_dump(by_alias=True, exclude_none=True),
        )
        logger.info(f"Created {ctx.parent.command.name} {role_obj.name}")
    else:
        logger.info(f"{ctx.parent.command.name.capitalize()} creation was cancelled")


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def edit(ctx: click.Context, resource_name: str):
    """Modify an existing {resource} interactively.

    This command starts an interactive session to guide you through
    modifying a {resource}, including its name and policies.

    \b
    Examples:
      # Start the interactive editor for '{example_name}'
      {full_command_prefix} edit {example_name}
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    json_data = json.loads(basic_show(profile, resource_path, resource_name))
    role_obj = Role(**json_data)
    role_to_update = modify_role_data_from_standard_input(profile, resource_path, role_obj)

    if not role_to_update:
        logger.info(f"{ctx.parent.command.name.capitalize()} update was cancelled.")
        return

    basic_update(
        profile,
        resource_path,
        resource_name=resource_name,
        body=role_to_update.model_dump(by_alias=True, exclude_none=True),
    )
    logger.info(f"Updated {ctx.parent.command.name} {role_to_update.name}")


@click.command(cls=HdxCommand, name="add-user")
@click.argument("resource_name")
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
def add_user(ctx: click.Context, resource_name: str, users):
    """Add one or more users to a {resource}.

    \b
    Examples:
      # Add 'user@example.com' to the '{example_name}' {resource}
      {full_command_prefix} add-user {example_name} --user user@example.com
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _manage_users_from_role(profile, resource_path, resource_name, users, action="add")
    logger.info(f"Added user(s) to {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand, name="remove-user")
@click.argument("resource_name")
@click.option(
    "-u",
    "--user",
    "users",
    multiple=True,
    required=True,
    help="Specify users to remove from a role (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove_user(ctx: click.Context, resource_name: str, users):
    """Remove one or more users from a {resource}.

    \b
    Examples:
      # Remove 'user@example.com' from the '{example_name}' {resource}
      {full_command_prefix} remove-user {example_name} --user user@example.com
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _manage_users_from_role(profile, resource_path, resource_name, users, action="remove")
    logger.info(f"Removed user(s) from {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand, name="list-permissions")
@click.option(
    "--scope-type",
    "-t",
    "scope_type",
    metavar="SCOPE_TYPE",
    required=False,
    default=None,
    help="Filter the permissions by a specific scope type.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_permissions(ctx: click.Context, scope_type: str):
    """Lists all available permissions that can be assigned
    to a role, optionally filtered by a scope type.

    \b
    Examples:
      # List all permissions available for the 'project' scope
      {full_command_prefix} list-permissions --scope-type project
    """
    profile = ctx.parent.obj["usercontext"]
    permissions_role = find_permissions(profile)

    if scope_type:
        permissions_role = [p for p in permissions_role if p.get("scope_type") == scope_type]

    if not permissions_role:
        message = "No permissions found."
        if scope_type:
            message = f"No permissions found for scope type '{scope_type}'."
        logger.info(message)
        return

    table = Table(box=None, show_header=True, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Scope Type", style="dim", no_wrap=True)
    table.add_column("Permissions")

    for resource in sorted(permissions_role, key=lambda x: x.get("scope_type")):
        scope = resource.get("scope_type")
        perms = resource.get("permissions", [])

        if not perms:
            table.add_row(scope, "[dim]No permissions for this scope.[/dim]")
            continue

        permission_renderable = Columns(perms, equal=True, column_first=True)
        table.add_row(scope, permission_renderable)

    console.print(table)


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
role.add_command(add_user)
role.add_command(remove_user)
role.add_command(command_delete)
role.add_command(command_show)
role.add_command(list_permissions)
