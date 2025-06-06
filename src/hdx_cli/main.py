from pathlib import Path

import click
from trogon import tui

from hdx_cli.auth.context_builder import load_user_context
from hdx_cli.cli_interface.check_health import commands as check_health_
from hdx_cli.cli_interface.credential import commands as credentials_
from hdx_cli.cli_interface.dictionary import commands as dictionary_
from hdx_cli.cli_interface.function import commands as function_
from hdx_cli.cli_interface.integration import commands as integration_
from hdx_cli.cli_interface.job import commands as job_
from hdx_cli.cli_interface.migrate import commands as migrate_
from hdx_cli.cli_interface.pool import commands as pool_
from hdx_cli.cli_interface.profile import commands as profile_
from hdx_cli.cli_interface.project import commands as project_
from hdx_cli.cli_interface.query_option import commands as query_option_
from hdx_cli.cli_interface.role import commands as role_
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
from hdx_cli.config.initial_setup import first_time_use_config, is_first_time_use
from hdx_cli.library_api.common.config_constants import PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.exceptions import ConfigurationExistsException
from hdx_cli.library_api.common.logging import get_logger, set_debug_logger, set_info_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.models import DEFAULT_TIMEOUT, ProfileLoadContext

VERSION = "1.0.80"

logger = get_logger()


def configure_logger(debug=False):
    if debug:
        set_debug_logger()
        return
    set_info_logger()


# pylint: disable=line-too-long
@tui(help="Open textual user interface")
@click.group(
    help="hdxcli is a tool to perform operations against Hydrolix cluster resources such as tables,"
    + " projects and transforms via different profiles. hdxcli supports profile configuration management "
    + " to perform operations on different profiles and sets of projects and tables."
)
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
    "--profile-config-file",
    type=click.Path(path_type=Path),
    hidden=True,
    default=PROFILE_CONFIG_FILE,
    help="Used only for testing.",
)
@click.option(
    "--uri-scheme",
    default=None,
    type=click.Choice(["http", "https"]),
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
    profile_config_file: Path,
    uri_scheme: str,
    timeout: int,
    debug: bool,
):
    """
    Command-line entry point for hdx cli interface
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
        "profile_config_file": profile_config_file,
        "uri_scheme": uri_scheme,
        "timeout": timeout,
    }
    ctx.obj["useroptions"] = user_options


@click.command(help="Initialize hdxcli configuration")
@report_error_and_exit(exctype=Exception)
def init():
    if not is_first_time_use():
        raise ConfigurationExistsException(
            "Configuration already exists for accessing your Hydrolix cluster. "
            "Please run the 'edit' command to update the configuration."
        )

    profile_load_ctx = first_time_use_config()
    if profile_load_ctx:
        load_user_context(profile_load_ctx)


@click.command(help="Print hdxcli version")
def version():
    logger.info(VERSION)


hdx_cli.add_command(init)
hdx_cli.add_command(project_.project)
hdx_cli.add_command(table_.table)
hdx_cli.add_command(shadow_.shadow)
hdx_cli.add_command(transform_.transform)
hdx_cli.add_command(view_.view)
hdx_cli.add_command(set_commands.set_default_resources)
hdx_cli.add_command(set_commands.unset_default_resources)
hdx_cli.add_command(job_.job)
hdx_cli.add_command(stream_.stream)
hdx_cli.add_command(function_.function)
hdx_cli.add_command(dictionary_.dictionary)
hdx_cli.add_command(storage_.storage)
hdx_cli.add_command(pool_.pool)
hdx_cli.add_command(profile_.profile)
hdx_cli.add_command(sources_.sources)
hdx_cli.add_command(migrate_.migrate)
hdx_cli.add_command(integration_.integration)
hdx_cli.add_command(user_.user)
hdx_cli.add_command(service_account_.service_account)
hdx_cli.add_command(role_.role)
hdx_cli.add_command(query_option_.query_option)
hdx_cli.add_command(credentials_.credential)
hdx_cli.add_command(check_health_.check_health)
hdx_cli.add_command(version)


def main():
    hdx_cli()  # pylint: disable=no-value-for-parameter


if __name__ == "__main__":
    main()
