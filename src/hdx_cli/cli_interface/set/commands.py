import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.config.profile_settings import load_static_profile_config, save_profile_config
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, with_profiles_context
from hdx_cli.models import ProfileLoadContext, ProfileUserContext

logger = get_logger()


@click.command(cls=HdxCommand, name="set")
@click.argument("project_name", metavar="PROJECT_NAME", required=True)
@click.argument("table_name", metavar="TABLE_NAME", required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def set_context(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    project_name: str,
    table_name: str,
):
    """Set the default project and table for the current profile.

    This command stores the provided project and table names in the current
    profile, allowing other commands to use them by default without needing
    the `--project` or `--table` options.

    \b
    Examples:
      # Set the default project
      {full_command_prefix} set web_proj

    \b
      # Set the default project and table
      {full_command_prefix} set web_proj dns_logs
    """
    user_context: ProfileUserContext = load_static_profile_config(profile_context)
    user_context.projectname = project_name
    user_context.tablename = table_name
    save_profile_config(user_context, initial_profile=config_profiles, logout=False)

    logger.info(f"Default context set for profile '{user_context.profilename}'")


@click.command(cls=HdxCommand, name="unset")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def unset_context(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
):
    """Clear the default project and table from the current profile.

    \b
    Examples:
      # Unset the default project and table
      {full_command_prefix} unset
    """
    user_context: ProfileUserContext = load_static_profile_config(profile_context)

    user_context.projectname = None
    user_context.tablename = None
    save_profile_config(user_context, initial_profile=config_profiles, logout=False)
    logger.info(f"Default context cleared for profile '{user_context.profilename}'")
