import click
from functools import partial

from ...library_api.common.exceptions import (
    ProfileExistsException, InvalidHostnameException,
    InvalidUsernameException, InvalidSchemeException,
    ProfileNotFoundException, HdxCliException
)
from ...library_api.common.validation import is_valid_hostname, is_valid_username, is_valid_scheme
from ...library_api.utility.decorators import (
    report_error_and_exit,
    with_profiles_context,
    dynamic_confirmation_prompt
)
from ...library_api.common.profile import (
    save_profile, get_profile_data_from_standard_input,
    ProfileWizardInfo, delete_profile
)
from ...library_api.common.logging import get_logger

logger = get_logger()


@click.group(help="Profile-related operations")
@click.pass_context
def profile(ctx: click.Context):
    ctx.obj = {'profilecontext': ctx.parent.obj['profilecontext']}


@click.command(help='Show profile')
@click.argument('profile_name', default=None, required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_show(ctx: click.Context, profile_context, config_profiles, profile_name):
    profile_name = profile_context.profilename if not profile_name else profile_name
    profile_to_show = config_profiles.get(profile_name)
    if not profile_to_show:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    logger.info(f'Showing [{profile_name}]')
    logger.info('-' * 100)
    for cfg_key, cfg_val in profile_to_show.items():
        logger.info(f"{cfg_key}: {cfg_val}")


@click.command(help='List profiles')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_list(ctx: click.Context, profile_context, config_profiles):
    for cfg_name in config_profiles:
        logger.info(cfg_name)


@click.command(help='Edit profile')
@click.argument('profile_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_edit(ctx: click.Context, profile_context, config_profiles, profile_name: str):
    profile_to_edit = config_profiles.get(profile_name)
    if not profile_to_edit:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    logger.info(f'Editing [{profile_name}]')
    logger.info('-' * 100)
    username, hostname, scheme = (
        profile_to_edit['username'], profile_to_edit['hostname'], profile_to_edit['scheme']
    )

    edit_profile_data = get_profile_data_from_standard_input(
        hostname=hostname, username=username, http_scheme=scheme
    )
    if not edit_profile_data:
        logger.info('')
        logger.info('Configuration aborted.')
        return

    save_profile(
        username=edit_profile_data.username,
        hostname=edit_profile_data.hostname,
        profilename=profile_name,
        scheme=edit_profile_data.scheme,
        profile_config_file=profile_context.profile_config_file,
        initial_profile=config_profiles
    )
    logger.info(f"Edited profile '{profile_name}'")


@report_error_and_exit(exctype=Exception)
def validate_hostname(ctx, params, hostname: str) -> str:
    if hostname and not is_valid_hostname(hostname):
        raise InvalidHostnameException('Invalid host name format.')
    return hostname


@report_error_and_exit(exctype=Exception)
def validate_username(ctx, params, username: str) -> str:
    if username and not is_valid_username(username):
        raise InvalidUsernameException('Invalid user name format.')
    return username


@report_error_and_exit(exctype=Exception)
def validate_scheme(ctx, params, scheme: str) -> str:
    if scheme and not is_valid_scheme(scheme):
        raise InvalidSchemeException("Invalid scheme, expected values 'https' or 'http'.")
    return scheme


@click.command(help='Add a new profile')
@click.argument('profile_name', default=None, required=True)
@click.option('--hostname', callback=validate_hostname, default=None,
              help='Host name of the cluster.')
@click.option('--username', callback=validate_username, default=None,
              help='User name for the cluster.')
@click.option('--scheme', callback=validate_scheme, default=None,
              help='Protocol to use for the connection (http or https).')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_add(ctx: click.Context, profile_context, config_profiles,
                profile_name: str, hostname: str, username: str, scheme: str):
    if config_profiles.get(profile_name):
        raise ProfileExistsException(f"Profile '{profile_name}' already exists.")

    if hostname and username and scheme:
        edit_profile_data = ProfileWizardInfo(hostname=hostname, username=username, scheme=scheme)
    elif hostname or username or scheme:
        raise click.MissingParameter('All three parameters (hostname, username, and scheme) are required.')
    else:
        edit_profile_data = get_profile_data_from_standard_input()

    if not edit_profile_data:
        logger.info('')
        logger.info('Configuration aborted.')
        return

    save_profile(
        username=edit_profile_data.username,
        hostname=edit_profile_data.hostname,
        profilename=profile_name,
        scheme=edit_profile_data.scheme,
        profile_config_file=profile_context.profile_config_file,
        initial_profile=config_profiles
    )
    logger.info(f"Created profile '{profile_name}'")


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message='delete this resource',
    fail_message='Incorrect prompt input: resource was not deleted'
)


@click.command(help='Delete profile')
@click.argument('profile_name', default=None, required=True)
@click.option('--disable-confirmation-prompt', is_flag=True, show_default=True,
              help='Suppress confirmation to delete resource.',  default=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@with_profiles_context
def profile_delete(ctx: click.Context, profile_context, config_profiles,
                   profile_name: str, disable_confirmation_prompt: bool):
    if profile_name == 'default':
        raise HdxCliException('The default profile cannot be deleted.')

    profile_config_file = profile_context.profile_config_file
    if not config_profiles.get(profile_name):
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    delete_profile(
        profile_name,
        initial_profile=config_profiles,
        profile_config_file=profile_config_file
    )
    logger.info(f"Deleted profile '{profile_name}'")


profile.add_command(profile_list, name='list')
profile.add_command(profile_show, name='show')
profile.add_command(profile_edit, name='edit')
profile.add_command(profile_add, name='add')
profile.add_command(profile_delete, name='delete')
