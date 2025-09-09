import json
from typing import Dict, Any, Tuple

import click
from rich.console import Console
from rich.table import Table

from .utils import (
    create_service_account_token,
    create_service_account,
    validate_roles_exist,
    update_roles,
    revoke_all_service_account_tokens,
    set_token_as_auth,
)
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.cached_operations import find_service_accounts, find_users
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_show
from hdx_cli.library_api.common.exceptions import LogicException, ResourceNotFoundException
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.models import ProfileUserContext

console = Console()


def _print_token_details(token_data: Dict[str, Any]):
    """Helper function to print token details in a table."""
    expires_in_seconds = token_data.get("expires_in", 0)
    expires_in_days = expires_in_seconds // 86400

    table = Table(show_header=False, box=None, padding=(0, 1), pad_edge=False)
    table.add_column(style="dim", no_wrap=True)
    # The value column will fold its content if it's too long.
    table.add_column(overflow="fold")

    table.add_row("Access Token:", token_data.get("access_token", "[not found]"))
    table.add_row("Token Type:", token_data.get("token_type", "[not found]"))
    table.add_row("Expires In:", f"{expires_in_seconds} seconds (~{expires_in_days} days)")

    console.print(table)


@click.group(cls=HdxGroup, name="service-account")
@click.option(
    "--service-account",
    "--sa",
    "service_account_name",
    default=None,
    help="Perform an operation on the specified service account.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def service_account(ctx: click.Context, service_account_name: str):
    """Service accounts are non-human users designed for
    programmatic API access. This includes creating, listing,
    deleting, and managing roles and tokens for them."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=service_account_name)
    ctx.obj = {
        "resource_path": "/config/v1/service_accounts/",
        "usercontext": user_profile,
    }


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--role",
    "-r",
    "roles",
    required=True,
    multiple=True,
    metavar="ROLE",
    help="Role to assign. Can be specified multiple times.",
)
@click.option(
    "--generate-token",
    "generate_token_duration",
    is_flag=False,
    flag_value="",
    default=None,
    metavar="[DURATION]",
    help="Generate a token after creation. Optionally, provide a duration (e.g., '30d', '1y').",
)
@click.option(
    "--set-as-auth",
    is_flag=True,
    help="Set the generated token as the authentication method for the current profile. "
         "This will overwrite any existing credentials.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    roles: Tuple[str],
    generate_token_duration: str,
    set_as_auth: bool,
):
    """This command creates a new {resource} and assigns one
    or more roles to it. An access token can be generated
    immediately by using the `--generate-token` flag.

    \b
    Examples:
      # Create a {resource} with the 'super_admin' role
      {full_command_prefix} create {example_name} --role super_admin

    \b
      # Create a {resource} and generate a token valid for 90 days
      {full_command_prefix} create grafana_connector --role reporting_viewer --generate-token 90d

    \b
      # Create a {resource}, generate a token, and set it as the auth method
      {full_command_prefix} create user_connector --role automation_admin --generate-token 90d --set-as-auth
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    svc_account = create_service_account(user_profile, resource_name, list(roles), resource_path)
    click.echo(f"Created service account '{resource_name}'")

    if generate_token_duration is not None:
        svc_account_id = svc_account.get("uuid")
        if not svc_account_id:
            raise LogicException("Could not retrieve UUID after service account creation.")

        token_data = create_service_account_token(user_profile,
                                                  svc_account_id,
                                                  duration=generate_token_duration)
        click.echo("\nToken successfully generated:")
        _print_token_details(token_data)

        if set_as_auth:
            set_token_as_auth(user_profile, token_data)
            click.echo(
                f"\nUpdated profile '{user_profile.profilename}' to use this token for authentication"
            )


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_service_account(ctx: click.Context):
    """List all available {resource_plural}.
    Displays a table with the names of all {resource_plural} and the roles
    assigned to them.

    \b
    Examples:
      # List all {resource_plural} in the organization
      {full_command_prefix} list
    """
    profile = ctx.parent.obj.get("usercontext")
    svc_account_list = find_service_accounts(profile)
    if not svc_account_list:
        return

    user_list = find_users(profile)
    user_roles_by_uuid = {user.get("uuid"): user.get("roles", []) for user in user_list}

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name", min_width=30)
    table.add_column("Roles", overflow="fold")

    for sa in svc_account_list:
        sa_name = sa.get("name", "")
        sa_uuid = sa.get("uuid")
        sa_roles = user_roles_by_uuid.get(sa_uuid, [])
        table.add_row(sa_name, ", ".join(sa_roles))

    console.print(table)


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=False, default=None)
@click.option(
    "--duration",
    metavar="DURATION",
    help="Set token lifetime (e.g., '30d', '12h', '1y'). If not set, the API default is used.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Display the full token response in JSON format.",
)
@click.option(
    "--set-as-auth",
    is_flag=True,
    help="Set the generated token as the authentication method for the current profile. "
         "This will overwrite any existing credentials.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def generate_token(
    ctx: click.Context,
    resource_name: str,
    duration: str,
    as_json: bool,
    set_as_auth: bool,
):
    """Generate a new access token for a {resource}.
    The {resource} name can be specified via argument or the global `--sa` option.

    \b
    Examples:
      # Generate a token for 'grafana_connector' that expires in 30 days and set it as the auth method
      {full_command_prefix} generate-token grafana_connector --duration 30d --set-as-auth
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=resource_name)
    svc_account_name = user_profile.service_accountname
    if not svc_account_name:
        raise click.BadParameter(
            "Service account name is required. Use an argument or the global --sa option."
        )

    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, svc_account_name))
    svc_account_id = svc_account.get("uuid")

    token_data = create_service_account_token(user_profile, svc_account_id, duration)

    if as_json:
        console.print_json(data=token_data)
    else:
        click.echo("Token successfully generated:")
        _print_token_details(token_data)

    if set_as_auth:
        set_token_as_auth(user_profile, token_data)
        click.echo(
            f"\nUpdated profile '{user_profile.profilename}' to use this token for authentication"
        )


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--yes",
    is_flag=True,
    help="Bypass the confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def revoke_tokens(ctx: click.Context, resource_name: str, yes: bool):
    """Revoke all active tokens for a {resource}.

    This is a security-sensitive operation that invalidates all existing
    tokens for the specified {resource}, forcing any application
    using them to re-authenticate with a new token.

    \b
    Examples:
      # Revoke all tokens for '{example_name}' after a confirmation prompt
      {full_command_prefix} revoke-tokens {example_name}
    """
    if not yes:
        click.confirm(
            f"Are you sure you want to revoke all tokens for '{resource_name}'? "
            "This action cannot be undone",
            abort=True,
        )

    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    svc_account = json.loads(basic_show(user_profile, resource_path, resource_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    revoke_all_service_account_tokens(user_profile, svc_account_id)
    click.echo(f"All tokens for service account '{resource_name}' have been revoked")


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--role",
    "-r",
    "roles",
    required=True,
    multiple=True,
    metavar="ROLE",
    help="Role(s) to assign. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def assign_role(ctx: click.Context, resource_name: str, roles: Tuple[str]):
    """Assign one or more roles to a {resource}.

    \b
    Examples:
      # Assign the 'operator' role to the '{example_name}' {resource}
      {full_command_prefix} assign-role {example_name} --role operator
    """
    user_profile = ctx.parent.obj["usercontext"]
    validate_roles_exist(user_profile, list(roles))

    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, resource_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    update_roles(user_profile, svc_account_id, list(roles))
    click.echo(f"Added role(s) to '{resource_name}'")


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--role",
    "-r",
    "roles_to_remove",
    required=True,
    multiple=True,
    metavar="ROLE",
    help="Role(s) to remove. Can be used multiple times.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def remove_role(ctx: click.Context, resource_name: str, roles_to_remove: Tuple[str]):
    """Remove one or more roles from a {resource}.

    \b
    Examples:
      # Remove the 'super_admin' role from the '{example_name}' {resource}
      {full_command_prefix} remove-role {example_name} --role super_admin
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    svc_account = json.loads(basic_show(user_profile, resource_path, resource_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    # Find the service account in the user list to get its current roles
    users = find_users(user_profile)
    svc_account_user_data = next(
        (user for user in users if user.get("uuid") == svc_account_id), None
    )

    if not svc_account_user_data:
        raise ResourceNotFoundException(f"Could not retrieve user data for '{resource_name}'.")

    current_roles = set(svc_account_user_data.get("roles", []))
    if not current_roles:
        raise LogicException(f"Service account '{resource_name}' has no roles assigned.")

    # Check that all roles to be removed are currently assigned
    roles_to_remove_set = set(roles_to_remove)
    not_assigned = roles_to_remove_set - current_roles
    if not_assigned:
        raise ResourceNotFoundException(
            f"The following role(s) are not assigned: {', '.join(not_assigned)}."
        )

    update_roles(user_profile, svc_account_id, list(roles_to_remove), action="remove")
    click.echo(f"Removed role(s) from '{resource_name}'")


service_account.add_command(list_service_account, name="list")
service_account.add_command(create)
service_account.add_command(generate_token)
service_account.add_command(revoke_tokens)
service_account.add_command(assign_role)
service_account.add_command(remove_role)
service_account.add_command(command_delete)
service_account.add_command(command_show)
