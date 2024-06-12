import click

from ...library_api.common.exceptions import (ProfileExistsException, InvalidHostnameException,
                                              InvalidUsernameException, InvalidSchemeException,
                                              ProfileNotFoundException, HdxCliException)
from ...library_api.common.validation import is_valid_hostname, is_valid_username, is_valid_scheme
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.profile import (save_profile, get_profile_data_from_standard_input,
                                           get_profiles, ProfileWizardInfo, delete_profile)
from ...library_api.common.logging import get_logger

logger = get_logger()


@click.group(help="Profile-related operations")
@click.pass_context
def profile(ctx: click.Context):
    ctx.obj = {'usercontext': ctx.parent.obj['usercontext']}


@click.command(help='Show profile')
@click.argument('profile_name', default=None, required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_show(ctx: click.Context, profile_name):
    profile_name = ctx.parent.obj['usercontext'].profilename if not profile_name else profile_name
    config_profiles = get_profiles()
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
def profile_list(ctx: click.Context):
    config_profiles = get_profiles()
    for cfg_name in config_profiles:
        logger.info(cfg_name)


@click.command(help='Edit profile')
@click.argument('profile_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_edit(ctx: click.Context, profile_name: str):
    config_profiles = get_profiles()
    profile_to_edit = config_profiles.get(profile_name)
    if not profile_to_edit:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    logger.info(f'Editing [{profile_name}]')
    logger.info('-' * 100)
    username, hostname, scheme = (profile_to_edit['username'], profile_to_edit['hostname'], profile_to_edit['scheme'])

    edit_profile_data = get_profile_data_from_standard_input(hostname=hostname, username=username, http_scheme=scheme)
    if not edit_profile_data:
        logger.info('')
        logger.info('Configuration aborted.')
        return

    save_profile(username=edit_profile_data.username,
                 hostname=edit_profile_data.hostname,
                 profilename=profile_name,
                 scheme=edit_profile_data.scheme,
                 initial_profile=config_profiles)
    logger.info(f"Edited profile '{profile_name}'")


@report_error_and_exit(exctype=Exception)
def validate_hostname(ctx, params, hostname):
    if hostname and not is_valid_hostname(hostname):
        raise InvalidHostnameException('Invalid host name format.')
    return hostname


@report_error_and_exit(exctype=Exception)
def validate_username(ctx, params, username):
    if username and not is_valid_username(username):
        raise InvalidUsernameException('Invalid user name format.')
    return username


@report_error_and_exit(exctype=Exception)
def validate_scheme(ctx, params, scheme):
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
def profile_add(ctx: click.Context, profile_name: str, hostname: str, username: str, scheme: str):
    config_profiles = get_profiles()
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

    save_profile(username=edit_profile_data.username,
                 hostname=edit_profile_data.hostname,
                 profilename=profile_name,
                 scheme=edit_profile_data.scheme,
                 initial_profile=config_profiles)
    logger.info(f"Created profile '{profile_name}'")


@click.command(help='Delete profile')
@click.argument('profile_name', default=None, required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_delete(ctx: click.Context, profile_name):
    if profile_name == 'default':
        raise HdxCliException('The default profile cannot be deleted.')

    config_profiles = get_profiles()
    if not config_profiles.get(profile_name):
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.")

    delete_profile(profile_name, initial_profile=config_profiles)
    logger.info(f"Deleted profile '{profile_name}'")


profile.add_command(profile_list, name='list')
profile.add_command(profile_show, name='show')
profile.add_command(profile_edit, name='edit')
profile.add_command(profile_add, name='add')
profile.add_command(profile_delete, name='delete')
