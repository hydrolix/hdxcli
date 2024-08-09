from pathlib import Path
import click

import toml

from ...library_api.common.auth import load_profile
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.utility.decorators import report_error_and_exit, ensure_logged_in, with_profiles_context
from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger

logger = get_logger()


def _serialize_to_config_file(profile: ProfileUserContext,
                              config_file_path: Path):
    all_profiles = None
    with open(config_file_path, 'r', encoding='utf-8') as stream:
        all_profiles = toml.load(stream)
        all_profiles[profile.profilename] = profile.as_dict_for_config()
    with open(config_file_path, 'w', encoding='utf-8') as stream:
        toml.dump(all_profiles, stream)


@click.command(help='Set project and/or table to apply subsequent commands on it', name='set')
@click.argument('projectname', metavar='PROJECT_NAME', required=False, default=None)
@click.argument('tablename', metavar='TABLE_NAME', required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set_default_resources(ctx: click.Context,
                          projectname: str,
                          tablename: str):
    # Currently the condition below cannot happen due to the fact that both projectname and table
    # are positional arguments. I leave it here because I could change def __iter__(self):
    # in the future

    profile_context = ctx.parent.obj['profilecontext']
    # 'load_profile()' checks if the profile exists, otherwise it raises an exception
    profile: ProfileUserContext = load_profile(profile_context)

    if tablename and (not profile.projectname and not projectname):
        raise LogicException('In order to set the table name you must also have the project name '
                             'set either in your profile or set with this command explicitly')
    if not projectname:
        raise ResourceNotFoundException('No project/table names provided')

    profile.projectname = projectname
    if tablename:
        profile.tablename = tablename
    _serialize_to_config_file(profile, profile_context.profile_config_file)
    logger.info(f"Profile '{profile_context.profilename}' set project/table")


@click.command(help='Remove any set project/table', name='unset')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def unset_default_resources(ctx: click.Context):
    profile_context = ctx.parent.obj['profilecontext']
    # 'load_profile()' checks if the profile exists, otherwise it raises an exception
    profile: ProfileUserContext = load_profile(profile_context)

    profile.projectname = None
    profile.tablename = None

    _serialize_to_config_file(profile, profile_context.profile_config_file)
    logger.info(f"Profile '{profile_context.profilename}' unset project/table")
