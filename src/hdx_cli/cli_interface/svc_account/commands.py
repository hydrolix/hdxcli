"""Commands relative to service account."""
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
)
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

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column(style="dim", no_wrap=True)
    # The value column will fold its content if it's too long.
    table.add_column(overflow="fold")

    table.add_row("Access Token:", token_data.get("access_token", "[not found]"))
    table.add_row("Token Type:", token_data.get("token_type", "[not found]"))
    table.add_row("Expires In:", f"{expires_in_seconds} seconds (~{expires_in_days} days)")

    console.print(table)


@click.group(name="service-account")
@click.option(
    "--service-account",
    "--sa",
    "service_account_name",
    metavar="SA_NAME",
    default=None,
    help="Use the passed service account name for subsequent commands.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def service_account(ctx: click.Context, service_account_name: str):
    """
    Provides functionality for managing Service Accounts.

    \b
    Service Accounts are used for programmatic access to the Hydrolix API,
    ideal for automation. This command group includes commands to create,
    list, delete, and manage tokens and roles for them.
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=service_account_name)
    ctx.obj = {
        "resource_path": "/config/v1/service_accounts/",
        "usercontext": user_profile,
    }


@click.command()
@click.argument("service_account_name", required=True, metavar="SA_NAME")
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
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    service_account_name: str,
    roles: Tuple[str],
    generate_token_duration: str,
):
    """
    Create a new service account.

    \b
    This command creates a new service account and assigns one or more roles to it.
    An access token can also be generated immediately by using the --generate-token flag.

    \b
    Examples:
      # Create a service account 'my_sa' with the 'super_admin' role
      hdxcli service-account create my_sa --role super_admin

    \b
      # Create an account and generate a token with a 90-day expiration
      hdxcli service-account create my_sa --role read_only --generate-token 90d
    """
    # The 'generate_token_duration' parameter will be:
    # - None: if --generate-token is not used.
    # - "" (empty string): if --generate-token is used without a value.
    # - "30d": if --generate-token 30d is used.
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    svc_account = create_service_account(
        user_profile, service_account_name, list(roles), resource_path
    )
    click.echo(f"Created service account '{service_account_name}'")

    if generate_token_duration is not None:
        svc_account_id = svc_account.get("uuid")
        if not svc_account_id:
            raise LogicException("Could not retrieve UUID after service account creation.")

        token_data = create_service_account_token(
            user_profile, svc_account_id, duration=generate_token_duration
        )
        click.echo("\nToken successfully generated:")
        _print_token_details(token_data)


@click.command(name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_service_account(ctx: click.Context):
    """
    Lists all available service accounts.

    \b
    Displays a table with the names of all service accounts and the roles
    assigned to them.
    """
    profile = ctx.parent.obj.get("usercontext")
    svc_account_list = find_service_accounts(profile)
    if not svc_account_list:
        return

    user_list = find_users(profile)
    user_roles_by_uuid = {user.get("uuid"): user.get("roles", []) for user in user_list}

    table = Table(show_header=True, box=None, padding=(0, 2), header_style="bold")
    table.add_column("Name", min_width=30)
    table.add_column("Roles", overflow="fold")

    for sa in svc_account_list:
        sa_name = sa.get("name", "")
        sa_uuid = sa.get("uuid")
        sa_roles = user_roles_by_uuid.get(sa_uuid, [])
        table.add_row(sa_name, ", ".join(sa_roles))

    console.print(table)


@click.command()
@click.argument("service_account_name", type=str, required=False, default=None, metavar="SA_NAME")
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
@click.pass_context
@report_error_and_exit(exctype=Exception)
def generate_token(ctx: click.Context, service_account_name: str, duration: str, as_json: bool):
    """
    Generates a new access token for a service account.

    \b
    The service account can be specified via argument or the global --sa option.

    \b
    Example:
      # Generate a token for 'my_sa' that expires in 30 days
      hdxcli service-account generate-token my_sa --duration 30d
    """
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, service_accountname=service_account_name)
    svc_account_name = user_profile.service_accountname
    if not svc_account_name:
        raise click.BadParameter(
            "Service account name is required. Use an argument or the --service-account option."
        )

    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, svc_account_name))
    svc_account_id = svc_account.get("uuid")

    token_data = create_service_account_token(user_profile, svc_account_id, duration)

    if as_json:
        console.print_json(data=token_data)
        return

    click.echo("Token successfully generated:")
    _print_token_details(token_data)


@click.command()
@click.argument("service_account_name", required=True, metavar="SA_NAME")
@click.option(
    "--yes",
    is_flag=True,
    help="Bypass the confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def revoke_tokens(ctx: click.Context, service_account_name: str, yes: bool):
    """
    Revokes all active tokens for a service account.

    \b
    This is a security-sensitive operation that invalidates all existing
    tokens for the specified service account, forcing any application
    using them to re-authenticate with a new token.

    \b
    This action cannot be undone and requires confirmation.
    """
    if not yes:
        click.confirm(
            f"Are you sure you want to revoke all tokens for '{service_account_name}'? "
            "This action cannot be undone",
            abort=True,
        )

    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    svc_account = json.loads(basic_show(user_profile, resource_path, service_account_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    revoke_all_service_account_tokens(user_profile, svc_account_id)
    click.echo(f"All tokens for service account '{service_account_name}' have been revoked")


@click.command()
@click.argument("service_account_name", required=True, metavar="SA_NAME")
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
def assign_role(ctx: click.Context, service_account_name: str, roles: Tuple[str]):
    """
    Assigns one or more roles to a service account.

    \b
    Example:
      # Assign the 'operator' role to the 'my_sa' service account
      hdxcli service-account assign-role my_sa --role operator
    """
    user_profile = ctx.parent.obj["usercontext"]
    validate_roles_exist(user_profile, list(roles))

    resource_path = ctx.parent.obj["resource_path"]
    svc_account = json.loads(basic_show(user_profile, resource_path, service_account_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    update_roles(user_profile, svc_account_id, list(roles))
    click.echo(f"Added role(s) to '{service_account_name}'")


@click.command()
@click.argument("service_account_name", required=True, metavar="SA_NAME")
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
def remove_role(ctx: click.Context, service_account_name: str, roles_to_remove: Tuple[str]):
    """
    Removes one or more roles from a service account.

    \b
    Example:
      # Remove the 'super_admin' role from the 'my_sa' service account
      hdxcli service-account remove-role my_sa --role super_admin
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    svc_account = json.loads(basic_show(user_profile, resource_path, service_account_name))
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    # Find the service account in the user list to get its current roles
    users = find_users(user_profile)
    svc_account_user_data = next(
        (user for user in users if user.get("uuid") == svc_account_id), None
    )

    if not svc_account_user_data:
        raise ResourceNotFoundException(f"Could not retrieve user data for '{service_account_name}'.")

    current_roles = set(svc_account_user_data.get("roles", []))
    if not current_roles:
        raise LogicException(f"Service account '{service_account_name}' has no roles assigned.")

    # Check that all roles to be removed are currently assigned
    roles_to_remove_set = set(roles_to_remove)
    not_assigned = roles_to_remove_set - current_roles
    if not_assigned:
        raise ResourceNotFoundException(
            f"The following role(s) are not assigned: {', '.join(not_assigned)}."
        )

    update_roles(user_profile, svc_account_id, list(roles_to_remove), action="remove")
    click.echo(f"Removed role(s) from '{service_account_name}'")


service_account.add_command(list_service_account, name="list")
service_account.add_command(create)
service_account.add_command(generate_token)
service_account.add_command(revoke_tokens)
service_account.add_command(assign_role)
service_account.add_command(remove_role)
service_account.add_command(command_delete)
service_account.add_command(command_show)
