import dataclasses as dc
import logging
from datetime import datetime

import click
from trogon import tui

from hdx_cli.cli_interface.project import commands as project_
from hdx_cli.cli_interface.table import commands as table_
from hdx_cli.cli_interface.transform import commands as transform_
from hdx_cli.cli_interface.job import commands as job_
from hdx_cli.cli_interface.stream import commands as stream_
from hdx_cli.cli_interface.function import commands as function_
from hdx_cli.cli_interface.dictionary import commands as dictionary_
from hdx_cli.cli_interface.storage import commands as storage_
from hdx_cli.cli_interface.profile import commands as profile_
from hdx_cli.cli_interface.pool import commands as pool_
from hdx_cli.cli_interface.sources import commands as sources_
from hdx_cli.cli_interface.migrate import commands as migrate_
from hdx_cli.cli_interface.integration import commands as integration_
from hdx_cli.cli_interface.user import commands as user_
from hdx_cli.cli_interface.role import commands as role_
from hdx_cli.cli_interface.query_option import commands as query_option_

from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.common.context import ProfileUserContext, ProfileLoadContext, DEFAULT_TIMEOUT
from hdx_cli.library_api.common.exceptions import HdxCliException, TokenExpiredException
from hdx_cli.library_api.common.config_constants import HDX_CONFIG_DIR, PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.first_use import try_first_time_use
from hdx_cli.library_api.common.profile import save_profile, get_profile_data_from_standard_input

from hdx_cli.library_api.common.auth_utils import load_user_context
from hdx_cli.library_api.common.logging import set_debug_logger, set_info_logger, get_logger

VERSION = "1.0-rc49"


from hdx_cli.library_api.common.auth import (
    load_profile,
    save_profile_cache,
    try_load_profile_from_cache_data)

from hdx_cli.cli_interface.set import commands as set_commands
from hdx_cli.library_api.common.login import login


logger = get_logger()


def _first_time_use_config(profile_config_file):
    logger.info('No configuration was found to access your hydrolix cluster.')
    logger.info('A new configuration will be created now.')
    logger.info('')
    profile_wizard_info = get_profile_data_from_standard_input()
    if not profile_wizard_info:
        logger.info('Configuration creation aborted')
        return
    save_profile(profile_wizard_info.username,
                 profile_wizard_info.hostname,
                 profile_config_file,
                 'default',
                 scheme=profile_wizard_info.scheme)
    logger.info('')
    logger.info(f'Your configuration with profile [default] has been created at {profile_config_file}')
    logger.info('-' * 100)


def _chain_calls_ignore_exc(*funcs, **kwargs):
    """Chain calls and return on_error_return on failure. Exceptions are considered failure if
    they derived from provided kwarg 'exctype', otherwise they will escape.
    on_error_default kwarg is what is returned in case of failure.

    Function parameters to funcs[0] are provided in kwargs, and subsequent results
    are are provided as input of the first function.

    """
    # Setup function
    exctype = kwargs.get('exctype', Exception)
    on_error_return = kwargs.get('on_error_return', None)
    try:
        del kwargs['exctype']
    except: # pylint:disable=bare-except
        pass
    try:
        del kwargs['on_error_return']
    except: # pylint:disable=bare-except
        pass

    # Run function
    try:
        result = funcs[0](**kwargs)
        if len(funcs) > 1:
            for func in funcs[1:]:
                result = func(result)
        return result
    except exctype:
        return on_error_return


def load_set_config_parameters(user_context: ProfileUserContext,
                               load_context: ProfileLoadContext):
    """Given a profile to load and an old profile, it returns the user_context
    with the config parameters projectname and tablename loaded."""
    config_params = {'projectname':
                     (prof := load_profile(load_context)).projectname,
                     'tablename':
                     prof.tablename,
                     'scheme': prof.scheme}
    user_ctx_dict = dc.asdict(user_context) | config_params
    # Keep old auth since asdict will transform AuthInfo into a dictionary.
    old_auth = user_context.auth
    new_user_ctx = ProfileUserContext(**user_ctx_dict)
    # And reassign when done
    new_user_ctx.auth = old_auth
    return new_user_ctx


def fail_if_token_expired(user_context: ProfileUserContext):
    if user_context.auth.expires_at <= datetime.now():
        raise TokenExpiredException()
    return user_context


def configure_logger(debug=False):
    if debug:
        set_debug_logger()
        return
    set_info_logger()


# pylint: disable=line-too-long
@tui(help='Open textual user interface')
@click.group(help='hdxcli is a tool to perform operations against Hydrolix cluster resources such as tables,' +
             ' projects and transforms via different profiles. hdxcli supports profile configuration management ' +
             ' to perform operations on different profiles and sets of projects and tables.')
@click.option('--profile', metavar='PROFILENAME', default=None,
              help="Perform operation with a different profile (default profile is 'default').")
@click.option('--password', metavar='PASSWORD', default=None,
              help="Login password. If provided and the access token is expired, it will be used.")
@click.option('--profile-config-file', hidden=True, default=None,
              help='Used only for testing.')
@click.option('--uri-scheme', default='default', type=click.Choice(['default', 'http', 'https']),
              help='Scheme used.')
@click.option('--timeout', type=int, default=DEFAULT_TIMEOUT,
              help=f'Set request timeout in seconds (default: {DEFAULT_TIMEOUT}).')
@click.option('--debug', hidden=True, is_flag=True, default=False,
              help=f'Enable debug mode, which displays additional information and '
                   f'debug messages for troubleshooting purposes.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint: enable=line-too-long
def hdx_cli(ctx, profile,
            password,
            profile_config_file,
            uri_scheme,
            timeout,
            debug):
    """
        Command-line entry point for hdx cli interface
    """
    configure_logger(debug)

    if ctx.invoked_subcommand == 'version':
        return

    profile = 'default' if not profile else profile
    profile_config_file = profile_config_file if profile_config_file else PROFILE_CONFIG_FILE

    try_first_time_use(_first_time_use_config, profile_config_file)

    load_context = ProfileLoadContext(profile, profile_config_file)

    user_context = load_user_context(load_context,
                                     password=password,
                                     profile_config_file=profile_config_file,
                                     uri_scheme=uri_scheme,
                                     timeout=timeout)
    # Unconditional default override
    ctx.obj = {'usercontext': user_context}


@click.command(help='Print hdxcli version')
def version():
    logger.info(VERSION)


hdx_cli.add_command(project_.project)
hdx_cli.add_command(table_.table)
hdx_cli.add_command(transform_.transform)
hdx_cli.add_command(set_commands.set)
hdx_cli.add_command(set_commands.unset)
hdx_cli.add_command(job_.job)
hdx_cli.add_command(stream_.stream)
hdx_cli.add_command(function_.function)
hdx_cli.add_command(job_.purgejobs)
hdx_cli.add_command(dictionary_.dictionary)
hdx_cli.add_command(storage_.storage)
hdx_cli.add_command(pool_.pool)
hdx_cli.add_command(profile_.profile)
hdx_cli.add_command(sources_.sources)
hdx_cli.add_command(migrate_.migrate)
hdx_cli.add_command(integration_.integration)
hdx_cli.add_command(user_.user)
hdx_cli.add_command(role_.role)
hdx_cli.add_command(query_option_.query_option)
hdx_cli.add_command(version)


def main():
    hdx_cli() # pylint: disable=no-value-for-parameter


if __name__ == '__main__':
    main()
