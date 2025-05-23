import click

from ...config.profile_settings import load_static_profile_config, save_profile_config
from ...library_api.common.exceptions import ResourceNotFoundException
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import report_error_and_exit, with_profiles_context
from ...models import ProfileLoadContext, ProfileUserContext

logger = get_logger()


@click.command(help="Set project and/or table to apply subsequent commands on it", name="set")
@click.argument("projectname", metavar="PROJECT_NAME", required=False, default=None)
@click.argument("tablename", metavar="TABLE_NAME", required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def set_default_resources(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    projectname: str,
    tablename: str,
):
    profile_context = ctx.parent.obj["profilecontext"]
    user_context: ProfileUserContext = load_static_profile_config(profile_context)

    if not projectname:
        raise ResourceNotFoundException("No project/table names provided.")

    user_context.projectname = projectname
    user_context.tablename = tablename
    save_profile_config(user_context, initial_profile=config_profiles, logout=False)
    logger.info(f"Profile '{user_context.profilename}' set project/table")


@click.command(help="Remove any set project/table", name="unset")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def unset_default_resources(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
):
    profile_context = ctx.parent.obj["profilecontext"]
    user_context: ProfileUserContext = load_static_profile_config(profile_context)

    user_context.projectname = None
    user_context.tablename = None
    save_profile_config(user_context, initial_profile=config_profiles, logout=False)
    logger.info(f"Profile '{user_context.profilename}' unset project/table")
