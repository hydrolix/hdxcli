import os
import dataclasses as dc

from datetime import datetime
from pathlib import Path

import functools as ft
import sys

import click
import toml

from hdx_cli.cli_interface.project import commands as project_
from hdx_cli.cli_interface.table import commands as table_
from hdx_cli.cli_interface.transform import commands as transform_
from hdx_cli.cli_interface.job import commands as job_
from hdx_cli.cli_interface.function import commands as function_
from hdx_cli.cli_interface.dictionary import commands as dictionary_
from hdx_cli.cli_interface.profile import commands as profile_
from hdx_cli.cli_interface.sources import commands as sources_
from hdx_cli.cli_interface.migrate import commands as migrate_


from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.common.validation import is_valid_username, is_valid_hostname
from hdx_cli.library_api.common.cache import CacheDict
from hdx_cli.library_api.common.context import ProfileUserContext, ProfileLoadContext
from hdx_cli.library_api.common.exceptions import HdxCliException, TokenExpiredException
from hdx_cli.library_api.common.config_constants import HDX_CLI_HOME_DIR, PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.first_use import try_first_time_use
from hdx_cli.library_api.common.profile import save_profile


VERSION = "1.0-rc25"

from hdx_cli.library_api.common.auth import (
    load_profile,
    save_profile_cache,
    try_load_profile_from_cache_data)

from hdx_cli.cli_interface.set import commands as set_commands
from hdx_cli.library_api.common.login import login


def _first_time_use_config(profile_config_file):
    print('No configuration was found to access your hydrolix cluster.')
    print('A new configuration will be created now.')
    print()
    good_hostname = False
    hostname = None
    try:
        while not good_hostname:
            hostname = input('Please, type the host name of your cluster: ')
            good_hostname = is_valid_hostname(hostname)
            if not good_hostname:
                print('Invalid host name.')
        good_username = False
        username = None
        while not good_username:
            username = input('Please, type the user name of your cluster: ')
            good_username = is_valid_username(username)
        save_profile(username,
                     hostname,
                     profile_config_file,
                     'default')
        print(f'\nYour configuration with profile [default] has been created at {profile_config_file}')
        print('This will be the profile used to perform commands against by default')
        print('You can start working with hdx-cli now')
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(-1)


# def _save_profile_cache(a_profile: ProfileUserContext,
#                         *,
#                         token,
#                         expiration_time: datetime,
#                         org_id,
#                         token_type,
#                         cache_dir_path=None):
#     """
#     Save a cache file for this profile.
#     The profile cache file is saved in cache_dir_path
#     """
#     os.makedirs(cache_dir_path, mode=0o700, exist_ok=True)
#     username = a_profile.username
#     hostname = a_profile.hostname
#     with open(cache_dir_path / f'{a_profile.profilename}', 'w', encoding='utf-8') as f:
#         CacheDict.build_from_dict({'org_id': f'{org_id}',
#                                    'token':{'auth_token': token,
#                                             'token_type': token_type,
#                                             'expires_at': expiration_time},
#                                    'username': f'{username}',
#                                    'hostname': f'{hostname}'}).save_to_stream(f)


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

# pylint: disable=line-too-long
@click.group(help='hdx-cli is a tool to perform operations against Hydrolix cluster resources such as tables,' +
             ' projects and transforms via different profiles. hdx-cli supports profile configuration management ' +
             ' to perform operations on different profiles and sets of projects and tables.')
@click.option('--profile', help="Perform operation with a different profile. (Default profile is 'default')",
              metavar='PROFILENAME', default=None)
@click.option('--project', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--transform',
              help="Explicitly pass the transform name. If none is given, the default transform for the used table is used.",
              metavar='TRANSFORMNAME', default=None)
@click.option('--job',
              help="Perform operation on the passed jobname.",
              metavar='JOBNAME', default=None)
@click.option('--function',
              help="Perform operation on the passed function.",
              metavar='FUNCTIONNAME', default=None)
@click.option('--dictionary',
              help="Perform operation on the passed dictionary.",
              metavar='DICTIONARYNAME', default=None)
@click.option('--password', help="Login password. If provided and the access token is expired, it will be used.",
              metavar='PASSWORD', default=None)
@click.option('--profile-config-file', hidden=True, help='Used only for testing.',
              default=None)
@click.option('--source', help='Source for kinesis/kafka streams',
              default=None)
@click.option('--uri-scheme',
              help='Scheme used',
              type=click.Choice(['default', 'http', 'https']),
              default='default')
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint: enable=line-too-long
def hdx_cli(ctx, profile,
            project,
            table,
            transform,
            job,
            function,
            dictionary,
            password,
            profile_config_file,
            source,
            uri_scheme):
    "Command-line entry point for hdx cli interface"
    if ctx.invoked_subcommand == 'version':
        return

    try_first_time_use(_first_time_use_config,
                       profile_config_file if profile_config_file else PROFILE_CONFIG_FILE)

    load_context = ProfileLoadContext('default' if not profile else profile,
                                      profile_config_file if profile_config_file else PROFILE_CONFIG_FILE)
    user_context = load_profile(load_context)
    load_set_params = ft.partial(load_set_config_parameters,
                                 load_context=load_context)
    # Load profile from cache
    user_context = _chain_calls_ignore_exc(try_load_profile_from_cache_data,
                                           fail_if_token_expired,
                                           load_set_params,
                                           # Parameters to first function
                                           load_ctx=load_context,
                                           # _chain_calls_ignore_exc Function configuration
                                           exctype=HdxCliException)

    if not user_context:
        user_context: ProfileUserContext = load_profile(load_context)
        auth_info = login(user_context.username,
                          user_context.hostname,
                          password=password,
                          use_ssl=True if user_context.scheme == 'https' else False)
        user_context.auth = auth_info
        user_context.org_id = auth_info.org_id
        cache_dir_path = (Path(profile_config_file).parent
                          if profile_config_file else HDX_CLI_HOME_DIR)
        save_profile_cache(user_context,
                           token=auth_info.token,
                           expiration_time=auth_info.expires_at,
                           token_type=auth_info.token_type,
                           org_id=auth_info.org_id,
                           cache_dir_path=cache_dir_path)
        user_context.auth = auth_info

    if uri_scheme != 'default':
        user_context.scheme = uri_scheme
    # Command-line overrides
    if transform:
        user_context.transformname = transform
    if job:
        user_context.batchname = job
    if project:
        user_context.projectname = project
    if table:
        user_context.tablename = table
    if function:
        user_context.functionname = function
    if dictionary:
        user_context.dictionaryname = dictionary
    if source:
        user_context.kafkaname = source
        user_context.kinesisname = source
    # Unconditional default override
    ctx.obj = {'usercontext': user_context}


@click.command(help='Print hdxcli version.')
def version():
    print(VERSION)


hdx_cli.add_command(project_.project)
hdx_cli.add_command(table_.table)
hdx_cli.add_command(transform_.transform)
hdx_cli.add_command(set_commands.set)
hdx_cli.add_command(set_commands.unset)
hdx_cli.add_command(job_.job)
hdx_cli.add_command(function_.function)
hdx_cli.add_command(job_.purgejobs)
hdx_cli.add_command(dictionary_.dictionary)
hdx_cli.add_command(profile_.profile)
hdx_cli.add_command(sources_.sources)
hdx_cli.add_command(migrate_.migrate)
hdx_cli.add_command(version)


def main():
    hdx_cli() # pylint: disable=no-value-for-parameter


if __name__ == '__main__':
    main()
