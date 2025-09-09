import json
from functools import partial
from typing import Tuple

import click
from rich.console import Console
from rich.table import Table

from hdx_cli.cli_interface.common.cached_operations import find_users, find_invites_user
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_show,
    basic_delete,
    basic_create
)
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import ensure_logged_in, report_error_and_exit, dynamic_confirmation_prompt
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
@click.option(
    "--user",
    "user_email",
    default=None,
    help="Perform operation on the passed user.",
)
@click.pass_context
@ensure_logged_in
def user(ctx: click.Context, user_email: str):
    """This command handles the administration of user accounts.
    Provides functionality to list, show, delete, and manage roles
    for existing users, and to manage their invitations."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, useremail=user_email)
    ctx.obj = {"resource_path": "/config/v1/users/", "usercontext": user_profile}


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_users(ctx: click.Context):
    """List all {resource_plural}.

    Displays a list of all {resource_plural}, excluding service accounts.
    The output includes the user's email and their assigned roles.

    \b
    Examples:
      # List all users in the organization
      {full_command_prefix} list
    """
    profile = ctx.parent.obj.get("usercontext")
    user_list = find_users(profile)
    if not user_list:
        return

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name", min_width=30)
    table.add_column("Roles", overflow="fold")

    for user_ in user_list:
        if user_.get("is_service_account", False):
            continue

        user_email = user_.get("email", "")
        user_roles = user_.get("roles", [])
        table.add_row(user_email, ", ".join(user_roles))

    console.print(table)


@click.command(cls=HdxCommand)
@click.argument("resource_email", required=False, default=None, metavar="USER_EMAIL")
@click.option(
    "-i",
    "--indent",
    is_flag=True,
    default=False,
    help="Output in indented JSON format.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, resource_email: str, indent: bool):
    """Show details for a specific {resource}.

    Displays the full configuration of a specific {resource}.
    It will use the invite specified with the `--{resource}` option.

    \b
    Examples:
      # Show details for a specific {resource}
      {full_command_prefix} show {example_name}
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Prioritize the argument from the command line
    effective_name = resource_email
    if not effective_name:
        effective_name = getattr(profile, "useremail", None)

    if not effective_name:
        raise LogicException(f"No default {ctx.parent.command.name} found in profile.")

    logger.info(
        basic_show(profile, resource_path, effective_name, indent=indent, filter_field="email")
    )


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(cls=HdxCommand)
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    show_default=True,
    default=False,
    help="Suppress confirmation to delete the {resource}.",
)
@click.argument("resource_name", metavar="USER_EMAIL")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt: bool):
    """Permanently deletes the specified {resource}. This action
    is irreversible.

    \b
    Examples:
      # Delete the specified {resource} and bypass the confirmation prompt
      {full_command_prefix} delete {example_name}@example.com --disable-confirmation-prompt
    """
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get("resource_path")
    user_profile = ctx.parent.obj.get("usercontext")
    if basic_delete(user_profile, resource_path, resource_name, filter_field="email"):
        logger.info(f"Deleted {ctx.parent.command.name} {resource_name}")
    else:
        logger.info(f"Could not delete {ctx.parent.command.name} {resource_name}. Not found")


@click.command(name="assign-role", cls=HdxCommand)
@click.argument("resource_email", metavar="USER_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    default=None,
    required=True,
    help="Role to assign. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def assign(ctx: click.Context, resource_email: str, roles: list):
    """Assign one or more roles to a {resource}.
    This command adds roles to an existing {resource}.

    \b
    Examples:
      # Assign the 'operator' and 'read_only' roles to a user
      {full_command_prefix} assign-role {example_name}@example.com --role operator --role read_only
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    user_uuid = json.loads(basic_show(profile, resource_path, resource_email, filter_field="email")).get(
        "uuid"
    )

    resource_path = f"{resource_path}{user_uuid}/add_roles/"
    body = {"roles": roles}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Added role(s) to {resource_email}")


@click.command(name="remove-role", cls=HdxCommand)
@click.argument("resource_email", metavar="USER_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    default=None,
    required=True,
    help="Role to remove. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove(ctx: click.Context, resource_email: str, roles: list):
    """Remove one or more roles from a {resource}.
    This command removes existing roles from a {resource}.

    \b
    Examples:
      # Remove the 'super_admin' role from a {resource}
      {full_command_prefix} remove-role {example_name}@example.com --role super_admin
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    user_ = json.loads(basic_show(profile, resource_path, resource_email, filter_field="email"))
    user_uuid = user_.get("uuid")
    user_roles = user_.get("roles")

    if not user_uuid or not user_roles:
        raise LogicException(f"There was an error getting roles for {resource_email}.")

    set_roles_to_remove = set(roles)
    set_user_roles = set(user_roles)
    if not set_roles_to_remove.issubset(set_user_roles):
        raise LogicException(
            f"User {resource_email} lacks {list(set_roles_to_remove - set_user_roles)} "
            f"role(s) for removal."
        )

    resource_path = f"{resource_path}{user_uuid}/remove_roles/"
    body = {"roles": list(roles)}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Removed role(s) from {resource_email}")


@click.group(cls=HdxGroup)
@click.option(
    "--invite",
    "user_email",
    metavar="USER_EMAIL",
    default=None,
    help="Perform operation on the passed user.",
)
@click.pass_context
def invite(ctx: click.Context, user_email: str):
    """Provides commands for managing user invitations.
    Includes commands to send, resend, list, show, and delete user
    invitations.
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, useremail=user_email)
    ctx.obj = {"resource_path": "/config/v1/invites/", "usercontext": user_profile}


@click.command(cls=HdxCommand)
@click.argument("resource_email", metavar="INVITE_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    required=True,
    help="Role to assign to the new user. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def send(ctx: click.Context, resource_email: str, roles: Tuple[str]):
    """Create and send a new {resource}.
    Sends an email invitation to a new user with a specific set of roles.

    \b
    Examples:
      # Invite a new user with the 'operator' role
      {full_command_prefix} send {example_name}_user@example.com --role operator
    """
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")

    org_id = profile.org_id
    body = {"email": resource_email, "org": org_id, "roles": roles}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Sent invitation to {resource_email}")


@click.command(cls=HdxCommand)
@click.argument("resource_email", metavar="INVITE_EMAIL")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def resend(ctx: click.Context, resource_email: str):
    """Resend an existing {resource}.
    Resends an invitation to a user, typically when the original invitation
    has expired or was not received.

    \b
    Examples:
      # Resend an invitation to a user
      {full_command_prefix} resend {example_name}_pending_user@example.com
    """
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    invite_id = json.loads(basic_show(profile, resource_path, resource_email, filter_field="email")).get(
        "id"
    )
    resource_path = f"{resource_path}{invite_id}/resend_invite/"
    basic_create(profile, resource_path)
    logger.info(f"Resent invitation to {resource_email}")


@click.command(cls=HdxCommand, name="list")
@click.option("-p", "--pending", is_flag=True, default=False, help="List only pending invitations.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_invites(ctx: click.Context, pending: bool):
    """List all {resource_plural}.

    Displays a list of all user invitations, showing their email and status. The
    list can be filtered for only pending invitations with the `--pending` flag.

    \b
    Examples:
      # List all invitations, including claimed and pending
      {full_command_prefix} list

      # List only the invitations with a 'pending' status
      {full_command_prefix} list --pending
    """
    user_profile = ctx.parent.obj.get("usercontext")
    invites = find_invites_user(user_profile)
    if pending:
        invites = [user_invite for user_invite in invites if user_invite.get("status") == "pending"]

    if not invites:
        return

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Email", min_width=30)
    table.add_column("Status")

    for invite_ in invites:
        invite_name = invite_.get("email", "")
        invite_status = invite_.get("status", "")
        table.add_row(invite_name, invite_status)

    console.print(table)


user.add_command(list_users)
user.add_command(assign)
user.add_command(remove)
user.add_command(delete)
user.add_command(show)
user.add_command(invite)
invite.add_command(list_invites)
invite.add_command(show)
invite.add_command(delete)
invite.add_command(send)
invite.add_command(resend)
