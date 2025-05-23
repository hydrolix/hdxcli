"""Commands relative to service account."""

import json

import click

from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.utility.decorators import (
    ensure_logged_in,
    report_error_and_exit,
)
from ...models import ProfileUserContext
from ..common.cached_operations import find_roles, find_service_accounts, find_users
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create, basic_show, log_formatted_table_header
from .utils import (
    create_service_account,
    create_service_account_token,
    update_roles,
    validate_roles_exist,
)


@click.group(
    name="service-account",
    help="Service account related operations.",
)
@click.option(
    "--service-account",
    "--sa",
    "service_account_name",
    metavar="SERVICEACCOUNTNAME",
    default=None,
    help="Use the passed service account name.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def service_account(ctx: click.Context, service_account_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=service_account_name)
    ctx.obj = {
        "resource_path": f"/config/v1/service_accounts/",
        "usercontext": user_profile,
    }


@click.command(help="Create a service account.")
@click.argument("service_account_name", required=True, metavar="SERVICEACCOUNTNAME")
@click.option(
    "--role",
    "-r",
    "roles",
    required=True,
    multiple=True,
    default=None,
    help="Role to assign to the service account. "
    "It can be specified multiple times to assign multiple roles.",
)
@click.option(
    "--generate-token",
    "--gt",
    "generate_token_",
    is_flag=True,
    default=False,
    show_default=True,
    help="If set, generates a token for the service account after creation.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, service_account_name: str, roles: list, generate_token_: bool):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    svc_account = create_service_account(user_profile, service_account_name, roles, resource_path)
    click.echo(f"Created service account {service_account_name}")

    # Generate a token for the service account if requested
    if generate_token_ and (svc_account_id := svc_account.get("uuid")):
        token_data = create_service_account_token(user_profile, svc_account_id)
        click.echo("\nToken successfully generated:")
        click.echo(f"Access Token: {token_data.get('access_token', '[not found]')}")
        click.echo(
            f"Expires In: {token_data.get('expires_in', 0)} seconds (~{token_data.get('expires_in', 0) // 86400} days)"
        )
        click.echo(f"Token Type: {token_data.get('token_type', '[not found]')}")


@click.command(help="List service accounts.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_service_account(ctx: click.Context):
    profile = ctx.parent.obj.get("usercontext")
    svc_account_list = find_service_accounts(profile)
    if not svc_account_list:
        return

    user_list = find_users(profile)
    user_roles_by_uuid = {user.get("uuid"): user.get("roles", []) for user in user_list}

    log_formatted_table_header({"name": 40, "roles": 50})
    for sa in svc_account_list:
        sa_name = sa.get("name", "")
        sa_uuid = sa.get("uuid")
        sa_roles = user_roles_by_uuid.get(sa_uuid, [])
        click.echo(f'{sa_name.ljust(40)}{(", ".join(sa_roles)).ljust(50)}')


@click.command(help="Generate a new token for the service account.")
@click.argument(
    "service_account_name", type=str, required=False, default=None, metavar="SERVICEACCOUNTNAME"
)
@click.option(
    "--json",
    "-j",
    "json_",
    is_flag=True,
    default=False,
    show_default=True,
    help="Display the token in JSON format.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def generate_token(ctx: click.Context, service_account_name: str, json_: bool):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=service_account_name)
    svc_account_name = user_profile.service_accountname
    if not svc_account_name:
        raise click.BadParameter(
            "Service account name is required. Use --service-account or --sa option."
        )

    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, svc_account_name))
    svc_account_id = svc_account.get("uuid")

    token_data = create_service_account_token(user_profile, svc_account_id)
    click.echo("Token successfully generated:\n")
    if json_:
        click.echo(json.dumps(token_data, indent=4))
        return

    click.echo(f"Access Token: {token_data.get('access_token', '[not found]')}")
    click.echo(
        f"Expires In: {token_data.get('expires_in', 0)} seconds "
        f"(~{token_data.get('expires_in', 0) // 86400} days)"
    )
    click.echo(f"Token Type: {token_data.get('token_type', '[not found]')}")


@click.command(help="Assign roles to a service account.")
@click.argument("service_account_name", required=True, metavar="SERVICEACCOUNTNAME")
@click.option(
    "--role",
    "-r",
    "roles",
    required=True,
    multiple=True,
    default=None,
    help="Role(s) to assign to the service account. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def assign_role(ctx: click.Context, service_account_name: str, roles: list):
    user_profile = ctx.parent.obj["usercontext"]

    # Check that all requested roles exist before assigning them
    validate_roles_exist(user_profile, roles)

    # Get the service account ID
    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, service_account_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    # Assign the roles to the service account
    update_roles(user_profile, svc_account_id, roles)
    click.echo(f"Added role(s) to {service_account_name}")


@click.command(help="Remove roles from a service account.")
@click.argument("service_account_name", required=True, metavar="SERVICEACCOUNTNAME")
@click.option(
    "--role",
    "-r",
    "roles",
    required=True,
    multiple=True,
    default=None,
    help="Role(s) to remove from the service account. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove_role(ctx: click.Context, service_account_name: str, roles: list):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    # Get the service account ID
    svc_account = json.loads(basic_show(user_profile, resource_path, service_account_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    # Get current roles of the service account
    users = find_users(user_profile)
    svc_account_roles = None
    for user_ in users:
        if user_.get("uuid") == svc_account_id:
            svc_account_roles = user_.get("roles")
            break

    if not svc_account_roles:
        raise ResourceNotFoundException(
            f"No roles assigned to service account '{service_account_name}'."
        )

    # Check that all roles to be removed are currently assigned
    roles_to_remove = set(roles)
    current_roles = set(svc_account_roles)
    not_assigned = roles_to_remove - current_roles
    if not_assigned:
        not_assigned_str = ", ".join(not_assigned)
        raise ResourceNotFoundException(
            f"The following role(s) are not assigned to the service account: {not_assigned_str}."
        )

    # Remove roles from the service account
    update_roles(user_profile, svc_account_id, roles, action="remove")
    click.echo(f"Removed role(s) from {service_account_name}")


service_account.add_command(list_service_account, name="list")
service_account.add_command(create)
service_account.add_command(generate_token, name="generate-token")
service_account.add_command(assign_role, name="assign-role")
service_account.add_command(remove_role, name="remove-role")
service_account.add_command(command_delete)
service_account.add_command(command_show)
