import click
from rich.console import Console
from rich.table import Table

from hdx_cli.auth.session import delete_session_file
from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.profile.utils import get_profile_from_context
from hdx_cli.config.profile_settings import (
    profile_config_from_standard_input, 
    save_profile_config,
    delete_profile_config, 
    is_valid_scheme, 
    is_valid_hostname,
)
from hdx_cli.library_api.common.exceptions import (
    InvalidHostnameException,
    InvalidSchemeException, 
    ProfileExistsException, 
    HdxCliException,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    report_error_and_exit, 
    with_profiles_context,
)
from hdx_cli.models import ProfileLoadContext, ProfileUserContext, BasicProfileConfig

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
@click.pass_context
def profile(ctx: click.Context):
    """Manage connection profiles for your Hydrolix clusters."""
    ctx.obj = {"profilecontext": ctx.parent.obj["profilecontext"]}


@click.command(cls=HdxCommand, name="show")
@click.argument("profile_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_show(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
):
    """Show details for a specific {resource}.

    \b
    Examples:
      # Show the configuration for the 'default' profile
      {full_command_prefix} show default
    """
    profile_to_show = get_profile_from_context(config_profiles, profile_name)

    table = Table(show_header=False, box=None, padding=(0, 1), pad_edge=False)
    table.add_column(style="dim")
    table.add_column()

    for key, value in profile_to_show.items():
        table.add_row(f"{key.capitalize()}:", str(value))

    console.print(table)


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_list(ctx: click.Context, profile_context, config_profiles):
    """List all available {resource_plural}.

    \b
    Examples:
      # List all configured profiles
      {full_command_prefix} list
    """
    table = Table(show_header=True, box=None, header_style="bold")
    table.add_column("Name")
    table.add_column("Hostname")

    for name, config in config_profiles.items():
        table.add_row(f"{name}", config.get("hostname"))

    console.print(table)


@click.command(cls=HdxCommand, name="edit")
@click.argument("profile_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_edit(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
):
    """Interactively edit an existing {resource}.

    \b
    Examples:
      # Start the interactive editor for the 'default' profile
      {full_command_prefix} edit default
    """
    profile_to_update = get_profile_from_context(config_profiles, profile_name)

    hostname = profile_to_update.get("hostname", None)
    scheme = profile_to_update.get("scheme", None)
    edited_profile_config = profile_config_from_standard_input(hostname, scheme)

    if not edited_profile_config:
        logger.info("\nConfiguration aborted.")
        return

    new_profile_context = ProfileLoadContext(
        name=profile_name,
        config_file=profile_context.config_file,
    )
    user_context = ProfileUserContext(
        profile_context=new_profile_context,
        profile_config=edited_profile_config,
    )
    save_profile_config(user_context, initial_profile=config_profiles)
    logger.info(f"Edited profile '{profile_name}'")


@click.command(cls=HdxCommand, name="add")
@click.argument("profile_name")
@click.option("--hostname", help="Hostname of the cluster.")
@click.option(
    "--scheme",
    type=click.Choice(("http", "https"), case_sensitive=False),
    help="Protocol for the connection.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_add(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
    hostname: str,
    scheme: str,
):
    """Add a new {resource}. This command can be run interactively
    or by providing all options.

    \b
    Examples:
      # Start the interactive guide to create a new {resource}
      {full_command_prefix} add {example_name}

    \b
      # Add a new profile non-interactively
      {full_command_prefix} add {example_name} --hostname example.hydrolix.dev --scheme https
    """
    if profile_name in config_profiles:
        raise ProfileExistsException(f"Profile '{profile_name}' already exists.")
    
    if hostname and scheme:
        if not is_valid_hostname(hostname):
            raise InvalidHostnameException("Invalid host name format.")
        if not is_valid_scheme(scheme):
            raise InvalidSchemeException("Invalid scheme, expected values 'https' or 'http'.")
        config = {"hostname": hostname, "scheme": scheme}
        profile_config = BasicProfileConfig(**config)
    elif hostname or scheme:
        raise click.BadOptionUsage(
            "hostname", "Both --hostname and --scheme are required when either is provided."
        )
    else:
        profile_config = profile_config_from_standard_input()

    if not profile_config:
        logger.info("\nConfiguration aborted.")
        return

    user_context = ProfileUserContext(
        profile_context=profile_context,
        profile_config=profile_config,
    )
    save_profile_config(user_context, initial_profile=config_profiles)
    logger.info(f"Created profile '{profile_name}'")


@click.command(cls=HdxCommand, name="delete")
@click.argument("profile_name")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_delete(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
    yes: bool,
):
    """Delete a {resource}.

    \b
    Examples:
      # Delete the profile named '{example_name}'
      {full_command_prefix} delete {example_name}
    """
    if profile_name == "default":
        raise HdxCliException("The default profile cannot be deleted.")

    get_profile_from_context(config_profiles, profile_name)

    if not yes:
        click.confirm(
            f"Are you sure you want to delete the profile '{profile_name}'?",
            abort=True,
        )

    delete_profile_config(profile_context, config_profiles)
    logger.info(f"Deleted profile '{profile_name}'")


@click.command(cls=HdxCommand, name="logout")
@click.argument("profile_name")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_logout(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
    yes: bool,
):
    """Log out from a {resource} by deleting its session cache.

    \b
    Examples:
      # Log out from the 'default' profile, skipping confirmation
      {full_command_prefix} logout default --yes
    """
    if not yes:
        click.confirm(
            f"Are you sure you want to log out from the profile '{profile_name}'?",
            abort=True,
        )

    get_profile_from_context(config_profiles, profile_name)
    if delete_session_file(profile_context):
        logger.info(f"Successfully logged out from profile '{profile_name}'")
    else:
        logger.info(f"Profile '{profile_name}' is not logged in. No action taken.")


profile.add_command(profile_list)
profile.add_command(profile_show)
profile.add_command(profile_edit)
profile.add_command(profile_add)
profile.add_command(profile_delete)
profile.add_command(profile_logout)
