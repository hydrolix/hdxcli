import click
from pathlib import Path

import toml

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import LogicException
from ...library_api.common.context import ProfileUserContext, ProfileLoadContext
from ...library_api.common.auth import PROFILE_CONFIG_FILE


def _serialize_to_config_file(profile: ProfileUserContext,
                              config_file_path: Path):
    all_profiles = None
    with open(config_file_path, 'r', encoding='utf-8') as stream:
        all_profiles = toml.load(stream)
        all_profiles[profile.profilename] = profile.as_dict_for_config()
    with open(config_file_path, 'w', encoding='utf-8') as stream:
        toml.dump(all_profiles, stream)


@click.command(help='Set project and or/table to apply subsequent commands on it')
@click.argument('projectname', metavar='PROJECT', required=False, default=None)
@click.argument('tablename', metavar='TABLE', required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set(ctx, projectname, tablename, scheme=None):
    profile: ProfileUserContext = ctx.obj['usercontext']
    # Currently the condition below cannot happen due to the fact that both projectname and table
    # are positional arguments. I leave it here because I could change def __iter__(self):
    # in the future

    if tablename and (not profile.projectname and not projectname):
        raise LogicException('In order to set the table name you must also have the project name set either in your '
                             'profile or set with this command explicitly')
    if projectname:
        profile.projectname = projectname
    if tablename:
        profile.tablename = tablename
    _serialize_to_config_file(profile, profile.profile_config_file)


@click.command(help='Remove any set projects/tables')
@click.pass_context
def unset(ctx):
    profile = ctx.obj['usercontext']
    profile.tablename = None
    profile.projectname = None

    print(f"Profile '{profile.profilename}' unset project and table")
    _serialize_to_config_file(profile, profile.profile_config_file)
