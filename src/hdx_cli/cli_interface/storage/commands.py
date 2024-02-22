import json
from functools import partial
import click

from ..common.migration import migrate_a_storage
from ...library_api.utility.decorators import (report_error_and_exit,
                                               dynamic_confirmation_prompt)
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger
from ..common.undecorated_click_commands import basic_create_with_body_from_string
from ..common.undecorated_click_commands import basic_delete, basic_settings
from ..common.rest_operations import (list_ as command_list,
                                      show as command_show)

logger = get_logger()


@click.group(help="Storage-related operations")
@click.option('--storage', 'storage_name', help='Perform operation on the passed storage.',
              metavar='STORAGENAME', default=None)
@click.pass_context
def storage(ctx: click.Context,
            storage_name):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/storages/',
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      storagename=storage_name)


@click.command(help='Create storage.')
@click.argument('storage_filename')
@click.argument('storage_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           storage_filename: str,
           storage_name: str):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    with open(storage_filename, "r", encoding="utf-8") as file:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           storage_name, file.read())
    logger.info(f'Created storage {storage_name}')


_confirmation_prompt = partial(dynamic_confirmation_prompt,
                               prompt="Please type 'delete this resource' to delete: ",
                               confirmation_message='delete this resource',
                               fail_message='Incorrect prompt input: resource was not deleted')


@click.command(help='Delete resource.')
@click.option('--disable-confirmation-prompt',
              is_flag=True,
              help='Suppress confirmation to delete resource.', show_default=True, default=False)
@click.argument('resource_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str,
           disable_confirmation_prompt: bool):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get('resource_path')
    user_profile = ctx.parent.obj.get('usercontext')
    params = {"force_operation": True}
    if basic_delete(user_profile, resource_path, resource_name, params=params):
        logger.info(f'Deleted {resource_name}')
    else:
        logger.info(f'Could not delete {resource_name}. Not found')


@click.command(help="Get, set or list settings on a resource. When invoked with "
               "only the key, it retrieves the value of the setting. If retrieved "
               "with both key and value, the value for the key, if it exists, will "
               "be set.\n"
               "Otherwise, when invoked with no arguments, all the settings will be listed.")
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def settings(ctx: click.Context,
             key,
             value):
    resource_path = ctx.parent.obj.get("resource_path")
    user_profile = ctx.parent.obj.get("usercontext")
    params = {"force_operation": True}
    the_value = value
    if value:
        the_value = value
        if (stripped := value.strip()).startswith('[') and stripped.endswith(']'):
            the_value = json.loads(stripped)
    basic_settings(user_profile, resource_path, key, the_value, params=params)


@click.command(help='Migrate a storage.')
@click.argument('storage_name', metavar='STORAGE_NAME', required=True, default=None)
@click.option('-tp', '--target-profile', required=False, default=None)
@click.option('-h', '--target-cluster-hostname', required=False, default=None)
@click.option('-u', '--target-cluster-username', required=False, default=None)
@click.option('-p', '--target-cluster-password', required=False, default=None)
@click.option('-s', '--target-cluster-uri-scheme', required=False, default='https')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context,
            storage_name: str,
            target_profile,
            target_cluster_hostname,
            target_cluster_username,
            target_cluster_password,
            target_cluster_uri_scheme):
    if target_profile is None and not (target_cluster_hostname and target_cluster_username
                                       and target_cluster_password and target_cluster_uri_scheme):
        raise click.BadParameter('Either provide a --target-profile or all four target cluster options.')

    user_profile = ctx.parent.obj['usercontext']
    migrate_a_storage(user_profile,
                      storage_name,
                      target_profile,
                      target_cluster_hostname,
                      target_cluster_username,
                      target_cluster_password,
                      target_cluster_uri_scheme)
    logger.info(f'Migrated storage {storage_name}')


storage.add_command(command_list)
storage.add_command(create)
storage.add_command(delete)
storage.add_command(command_show)
storage.add_command(settings)
storage.add_command(migrate)
