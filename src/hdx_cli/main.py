from pathlib import Path

import click
from importlib.metadata import version, PackageNotFoundError
from trogon import tui

from hdx_cli.auth.context_builder import load_user_context
from hdx_cli.cli_interface.check_health import commands as check_health_
from hdx_cli.cli_interface.column import commands as column_
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.credential import commands as credentials_
from hdx_cli.cli_interface.defaults import commands as defaults_
from hdx_cli.cli_interface.dictionary import commands as dictionary_
from hdx_cli.cli_interface.function import commands as function_
from hdx_cli.cli_interface.integration import commands as integration_
from hdx_cli.cli_interface.job import commands as job_
from hdx_cli.cli_interface.migrate import commands as migrate_
from hdx_cli.cli_interface.pool import commands as pool_
from hdx_cli.cli_interface.profile import commands as profile_
from hdx_cli.cli_interface.project import commands as project_
from hdx_cli.cli_interface.query_option import commands as query_option_
from hdx_cli.cli_interface.resource_summary import commands as resource_summary_
from hdx_cli.cli_interface.role import commands as role_
from hdx_cli.cli_interface.row_policy import commands as row_policy_
from hdx_cli.cli_interface.set import commands as set_commands
from hdx_cli.cli_interface.shadow import commands as shadow_
from hdx_cli.cli_interface.sources import commands as sources_
from hdx_cli.cli_interface.storage import commands as storage_
from hdx_cli.cli_interface.stream import commands as stream_
from hdx_cli.cli_interface.svc_account import commands as service_account_
from hdx_cli.cli_interface.table import commands as table_
from hdx_cli.cli_interface.transform import commands as transform_
from hdx_cli.cli_interface.user import commands as user_
from hdx_cli.cli_interface.view import commands as view_
from hdx_cli.cli_interface.docs import commands as docs_
from hdx_cli.config.initial_setup import first_time_use_config, is_first_time_use
from hdx_cli.library_api.common.config_constants import PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.exceptions import ConfigurationExistsException
from hdx_cli.library_api.common.logging import get_logger, set_debug_logger, set_info_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.models import DEFAULT_TIMEOUT, ProfileLoadContext

try:
    VERSION = version("hdxcli")
except PackageNotFoundError:
    VERSION = "0.0.0-dev"

logger = get_logger()


def configure_logger(debug=False):
    if debug:
        set_debug_logger()
        return
    set_info_logger()


# pylint: disable=line-too-long

@tui(help="Open a Textual User Interface (TUI) for the CLI.")
@click.group(cls=HdxGroup)
@click.option(
    "--profile",
    metavar="PROFILENAME",
    default="default",
    help="Perform operation with a different profile (default profile is 'default').",
)
@click.option(
    "--username",
    metavar="USERNAME",
    default=None,
    help="Login username. If it's the first login attempt or no active session exists, "
    "this username will be used (requires --password).",
)
@click.option(
    "--password",
    metavar="PASSWORD",
    default=None,
    help="Login password. If provided and the access token is expired, it will be used.",
)
@click.option(
    "--access-token",
    metavar="ACCESS_TOKEN",
    default=None,
    help="Provide a raw access token to use for authentication, bypassing all other methods.",
)
@click.option(
    "--profile-config-file",
    type=click.Path(path_type=Path),
    hidden=True,
    default=PROFILE_CONFIG_FILE,
    help="Used only for testing.",
)
@click.option(
    "--uri-scheme",
    default=None,
    type=click.Choice(["http", "https"], case_sensitive=False),
    help="Specify the URI scheme to use.",
)
@click.option(
    "--timeout",
    type=int,
    default=DEFAULT_TIMEOUT,
    help=f"Set request timeout in seconds (default: {DEFAULT_TIMEOUT}).",
)
@click.option(
    "--debug",
    hidden=True,
    is_flag=True,
    default=False,
    help="Enable debug mode, which displays additional information and "
    "debug messages for troubleshooting purposes.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def hdx_cli(
    ctx,
    profile: str,
    username: str,
    password: str,
    access_token: str,
    profile_config_file: Path,
    uri_scheme: str,
    timeout: int,
    debug: bool,
):
    """
    The official command-line interface for Hydrolix.

    \b
    `hdxcli` allows you to interact with and manage your Hydrolix cluster
    resources, such as projects, tables, users, and service accounts,
    directly from your terminal.

    \b
    It uses connection profiles to manage different cluster environments
    and provides a comprehensive set of commands for resource management
    and administration.

    Use 'hdxcli [COMMAND] --help' for more information on a specific command.
    """
    configure_logger(debug)
    if ctx.invoked_subcommand in ("version", "init"):
        return

    # Check if the profile configuration file exists
    if is_first_time_use(profile_config_file):
        profile_context = first_time_use_config(profile_config_file)
    else:
        profile_context = ProfileLoadContext(profile, profile_config_file)

    ctx.obj = {"profilecontext": profile_context}
    user_options = {
        "username": username,
        "password": password,
        "access_token": access_token,
        "profile_config_file": profile_config_file,
        "uri_scheme": uri_scheme,
        "timeout": timeout,
    }
    ctx.obj["useroptions"] = user_options


@click.command(cls=HdxCommand, name="init")
@report_error_and_exit(exctype=Exception)
def init():
    """
    Initialize the HDXCLI configuration for first-time use.

    This command guides you through creating the initial configuration
    file and setting up your 'default' profile. It is intended to be
    run only once. If a configuration already exists, the command
    will exit with an error to prevent overwriting settings.

    \f
    **Example Output**

    The following is a sample of the interactive `init` session:
    ```
    $ hdxcli init
    No configuration found for your Hydrolix cluster.
    Let's create the 'default' profile to get you started.

    ----- Configuring Profile [default] -----

    ? Enter the host address for the profile: host.hydrolix.dev
    ? Use TLS (https) for connection? Yes

    The configuration for 'default' profile has been created.
    Please login to profile 'default' (host.hydrolix.dev) to continue.
    Username: user@hydrolix.io
    Password for [user@hydrolix.io]:

    ----- Service Account Configuration -----
    A Service Account can be configured for automated access.
    ? How would you like to authenticate for this profile? Create a new Service Account
    ? What is the name for the new Service Account? user_sa
    ? Select roles to assign: ['user_admin']
    ? Enter token duration (e.g., 30d, 1y) or leave blank for default (1 year): 180d

    Profile 'default' is now configured to use Service Account 'user_sa'.
    ----- End of Service Account Configuration -----

    Configuration complete. You can now use hdxcli to manage your cluster.
    ```
    """
    if not is_first_time_use():
        raise ConfigurationExistsException(
            "Configuration already exists. "
            "Use 'hdxcli profile edit' to modify an existing profile."
        )

    profile_load_ctx = first_time_use_config()
    if profile_load_ctx:
        user_context = load_user_context(profile_load_ctx)
        if user_context:
            logger.info("Configuration complete. You can now use hdxcli to manage your cluster.")


@click.command(help="Print hdxcli version")
def version():
    logger.info(VERSION)


hdx_cli.add_command(init)
hdx_cli.add_command(project_.project)
hdx_cli.add_command(table_.table)
hdx_cli.add_command(shadow_.shadow)
hdx_cli.add_command(transform_.transform)
hdx_cli.add_command(row_policy_.row_policy)
hdx_cli.add_command(view_.view)
hdx_cli.add_command(column_.column)
hdx_cli.add_command(set_commands.set_context)
hdx_cli.add_command(set_commands.unset_context)
hdx_cli.add_command(job_.job)
hdx_cli.add_command(stream_.stream)
hdx_cli.add_command(function_.function)
hdx_cli.add_command(dictionary_.dictionary)
hdx_cli.add_command(storage_.storage)
hdx_cli.add_command(pool_.pool)
hdx_cli.add_command(profile_.profile)
hdx_cli.add_command(sources_.source)
hdx_cli.add_command(migrate_.migrate)
hdx_cli.add_command(integration_.integration)
hdx_cli.add_command(user_.user)
hdx_cli.add_command(service_account_.service_account)
hdx_cli.add_command(role_.role)
hdx_cli.add_command(query_option_.query_option)
hdx_cli.add_command(credentials_.credential)
hdx_cli.add_command(check_health_.check_health)
hdx_cli.add_command(resource_summary_.resource_summary)
hdx_cli.add_command(defaults_.show_defaults)
hdx_cli.add_command(version)
hdx_cli.add_command(docs_.docs)


def main():
    hdx_cli()  # pylint: disable=no-value-for-parameter


if __name__ == "__main__":
    main()
