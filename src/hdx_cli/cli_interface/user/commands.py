import json
from functools import partial

import click

from ...library_api.common.exceptions import LogicException
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    dynamic_confirmation_prompt,
    ensure_logged_in,
    report_error_and_exit,
)
from ...models import ProfileUserContext
from ..common.cached_operations import find_invites_user, find_users
from ..common.undecorated_click_commands import (
    basic_create,
    basic_delete,
    basic_show,
    log_formatted_table_header,
)

logger = get_logger()


@click.group(help="User-related operations")
@click.option(
    "--user",
    "user_email",
    metavar="USER_EMAIL",
    default=None,
    help="Perform operation on the passed user.",
)
@click.pass_context
@ensure_logged_in
def user(ctx: click.Context, user_email: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, useremail=user_email)
    ctx.obj = {"resource_path": "/config/v1/users/", "usercontext": user_profile}


@click.command(help="List users.", name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_users(ctx: click.Context):
    profile = ctx.parent.obj.get("usercontext")
    user_list = find_users(profile)
    if not user_list:
        return

    log_formatted_table_header({"email": 45, "roles": 50})
    for user_ in user_list:
        if user_.get("is_service_account", False):
            continue
        roles_name = user_.get("roles", "")
        logger.info(f'{user_.get("email", "").ljust(45)}{(", ".join(roles_name)).ljust(50)}')


@click.command(
    help="Show resource. If not resource_name is provided, it will show the default if there is one."
)
@click.option(
    "-i",
    "--indent",
    is_flag=True,
    default=False,
    help="Number of spaces for indentation in the output.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    if not (resource_name := getattr(profile, "useremail")):
        raise LogicException("No default user found in profile.")
    logger.info(
        basic_show(profile, resource_path, resource_name, indent=indent, filter_field="email")
    )


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(help="Delete resource.")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    show_default=True,
    default=False,
    help="Suppress confirmation to delete resource.",
)
@click.argument("email", metavar="USER_EMAIL")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, email: str, disable_confirmation_prompt: bool):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get("resource_path")
    user_profile = ctx.parent.obj.get("usercontext")
    if basic_delete(user_profile, resource_path, email, filter_field="email"):
        logger.info(f"Deleted {email}")
    else:
        logger.info(f"Could not delete {email}. Not found")


@click.command(name="assign-role", help="Assign roles to a user.")
@click.argument("email", metavar="USER_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    default=None,
    required=True,
    help="Specify roles to assign to a user (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def assign(ctx: click.Context, email: str, roles):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    user_uuid = json.loads(basic_show(profile, resource_path, email, filter_field="email")).get(
        "uuid"
    )

    resource_path = f"{resource_path}{user_uuid}/add_roles/"
    body = {"roles": roles}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Added role(s) to {email}")


@click.command(name="remove-role", help="Remove roles from a user.")
@click.argument("email", metavar="USER_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    default=None,
    required=True,
    help="Specify roles to remove from a user (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove(ctx: click.Context, email: str, roles: list):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    user_ = json.loads(basic_show(profile, resource_path, email, filter_field="email"))
    user_uuid = user_.get("uuid")
    user_roles = user_.get("roles")

    if not user_uuid or not user_roles:
        raise LogicException(f"There was an error getting roles for {email}.")

    set_roles_to_remove = set(roles)
    set_user_roles = set(user_roles)
    if not set_roles_to_remove.issubset(set_user_roles):
        raise LogicException(
            f"User {email} lacks {list(set_roles_to_remove - set_user_roles)} "
            f"role(s) for removal."
        )

    resource_path = f"{resource_path}{user_uuid}/remove_roles/"
    body = {"roles": list(roles)}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Removed role(s) from {email}")


@click.group(help="Invite-related operations")
@click.option(
    "--user",
    "user_email",
    metavar="USER_EMAIL",
    default=None,
    help="Perform operation on the passed user.",
)
@click.pass_context
def invite(ctx: click.Context, user_email: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, useremail=user_email)
    ctx.obj = {"resource_path": "/config/v1/invites/", "usercontext": user_profile}


@click.command(help="Send invitation to a new user.")
@click.argument("email", metavar="USER_EMAIL")
@click.option(
    "-r",
    "--role",
    "roles",
    multiple=True,
    default=None,
    required=True,
    help="Specify the role for the new user (can be used multiple times).",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def send(ctx: click.Context, email: str, roles):
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")

    org_id = profile.org_id
    body = {"email": email, "org": org_id, "roles": roles}
    basic_create(profile, resource_path, body=body)
    logger.info(f"Sent invitation to {email}")


@click.command(help="Resend invitation.")
@click.argument("email", metavar="USER_EMAIL")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def resend(ctx: click.Context, email: str):
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    invite_id = json.loads(basic_show(profile, resource_path, email, filter_field="email")).get(
        "id"
    )
    resource_path = f"{resource_path}{invite_id}/resend_invite/"
    basic_create(profile, resource_path)
    logger.info(f"Resent invitation to {email}")


@click.command(help="List invites.", name="list")
@click.option("-p", "--pending", is_flag=True, default=False, help="List only pending invitations.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_invites(ctx: click.Context, pending: bool):
    user_profile = ctx.parent.obj.get("usercontext")
    invites = find_invites_user(user_profile)
    if pending:
        invites = [user_invite for user_invite in invites if user_invite.get("status") == "pending"]

    if not invites:
        return

    log_formatted_table_header({"email": 45, "status": 30})
    for user_invite in invites:
        logger.info(
            f'{user_invite.get("email", "").ljust(45)}{user_invite.get("status", "").ljust(30)}'
        )


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
