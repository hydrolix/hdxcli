from functools import partial

import click

from ...auth.auth import is_valid_username
from ...auth.session import delete_session_file
from ...config.profile_settings import (
    delete_profile_config,
    is_valid_hostname,
    is_valid_scheme,
    profile_config_from_standard_input,
    save_profile_config,
)
from ...library_api.common.exceptions import (
    HdxCliException,
    InvalidHostnameException,
    InvalidSchemeException,
    InvalidUsernameException,
    ProfileExistsException,
    ProfileNotFoundException,
)
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    dynamic_confirmation_prompt,
    report_error_and_exit,
    with_profiles_context,
)
from ...models import BasicProfileConfig, ProfileLoadContext, ProfileUserContext

logger = get_logger()


@click.group(help="Profile-related operations")
@click.pass_context
def profile(ctx: click.Context):
    ctx.obj = {"profilecontext": ctx.parent.obj["profilecontext"]}


@click.command(help="Show profile")
@click.argument("profile_name", default=None, required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_show(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
):
    if not profile_name:
        raise click.MissingParameter("Profile name is required.")

    profile_to_show = config_profiles.get(profile_name)
    if not profile_to_show:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    logger.info(f"Showing [{profile_name}]")
    logger.info("-" * 50)
    for cfg_key, cfg_val in profile_to_show.items():
        logger.info(f"{cfg_key}: {cfg_val}")


@click.command(help="List profiles")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_list(ctx: click.Context, profile_context, config_profiles):
    for cfg_name in config_profiles:
        logger.info(cfg_name)


@click.command(help="Edit profile")
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
    profile_to_update = config_profiles.get(profile_name)
    if not profile_to_update:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    logger.info(f"Editing [{profile_name}]")
    logger.info("-" * 50)

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
        profile_context=new_profile_context, profile_config=edited_profile_config
    )
    save_profile_config(user_context, initial_profile=config_profiles)
    logger.info(f"Edited profile '{profile_name}'")


@report_error_and_exit(exctype=Exception)
def validate_hostname(ctx, params, hostname: str) -> str:
    if hostname and not is_valid_hostname(hostname):
        raise InvalidHostnameException("Invalid host name format.")
    return hostname


@report_error_and_exit(exctype=Exception)
def validate_username(ctx, params, username: str) -> str:
    if username and not is_valid_username(username):
        raise InvalidUsernameException("Invalid user name format.")
    return username


@report_error_and_exit(exctype=Exception)
def validate_scheme(ctx, params, scheme: str) -> str:
    if scheme and not is_valid_scheme(scheme):
        raise InvalidSchemeException("Invalid scheme, expected values 'https' or 'http'.")
    return scheme


@click.command(help="Add a new profile")
@click.argument("profile_name", default=None, required=True)
@click.option(
    "--hostname", callback=validate_hostname, default=None, help="Host name of the cluster."
)
@click.option(
    "--scheme",
    callback=validate_scheme,
    default=None,
    help="Protocol to use for the connection (http or https).",
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
    if config_profiles.get(profile_name):
        raise ProfileExistsException(f"Profile '{profile_name}' already exists.")

    if hostname and scheme:
        profile_config = BasicProfileConfig(hostname=hostname, scheme=scheme)
    elif hostname or scheme:
        raise click.MissingParameter("Both parameters (hostname and scheme) are required.")
    else:
        profile_config = profile_config_from_standard_input()

    if not profile_config:
        logger.info("\nConfiguration aborted.")
        return

    user_context = ProfileUserContext(
        profile_context=profile_context, profile_config=profile_config
    )
    save_profile_config(user_context, initial_profile=config_profiles)
    logger.info(f"Created profile '{profile_name}'")


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(help="Delete profile")
@click.argument("profile_name", default=None, required=True)
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    show_default=True,
    help="Suppress confirmation to delete resource.",
    default=False,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_delete(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
    disable_confirmation_prompt: bool,
):
    if profile_name == "default":
        raise HdxCliException("The default profile cannot be deleted.")

    if not config_profiles.get(profile_name):
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    prompt = f"Please type 'delete {profile_name}' to delete: "
    confirmation_msg = f"delete {profile_name}"
    dynamic_confirmation_prompt(
        prompt=prompt,
        confirmation_message=confirmation_msg,
        fail_message="Incorrect input. Profile was not deleted.",
        prompt_active=not disable_confirmation_prompt,
    )

    delete_profile_config(profile_context, config_profiles)
    logger.info(f"Deleted profile '{profile_name}'")


@click.command(help="Log out from a profile.")
@click.argument("profile_name")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    show_default=True,
    default=False,
    help="Suppress confirmation to log out and delete profile data.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_logout(
    ctx: click.Context,
    profile_context: ProfileLoadContext,
    config_profiles: dict,
    profile_name: str,
    disable_confirmation_prompt: bool,
):
    """
    Logs out from the specified profile by deleting its cache data.
    """
    if not config_profiles.get(profile_name):
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    prompt = f"Please type 'logout {profile_name}' to log out: "
    confirmation_msg = f"logout {profile_name}"
    dynamic_confirmation_prompt(
        prompt=prompt,
        confirmation_message=confirmation_msg,
        fail_message="Incorrect input. The profile was not logged out.",
        prompt_active=not disable_confirmation_prompt,
    )

    if not delete_session_file(profile_context):
        logger.info(f"Profile '{profile_name}' is not logged in. No action taken.")
        return
    logger.info(f"Successfully logged out from profile '{profile_name}'")


profile.add_command(profile_list, name="list")
profile.add_command(profile_show, name="show")
profile.add_command(profile_edit, name="edit")
profile.add_command(profile_add, name="add")
profile.add_command(profile_delete, name="delete")
profile.add_command(profile_logout, name="logout")
