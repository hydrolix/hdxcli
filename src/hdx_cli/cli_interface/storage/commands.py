import click
import json
from functools import partial

from ...library_api.utility.decorators import (report_error_and_exit,
                                               dynamic_confirmation_prompt)

from ..common.undecorated_click_commands import basic_create_with_body_from_string
from ..common.undecorated_click_commands import basic_delete, basic_settings
from ..common.rest_operations import (list_ as command_list,
                                      show as command_show)


@click.group(help="Storage-related operations")
@click.pass_context
def storage(ctx: click.Context):
    profile = ctx.parent.obj['usercontext']
    org_id = profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/storages/',
               'usercontext': profile}


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
    with open(storage_filename, "r", encoding="utf-8") as f:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           storage_name, f.read())
    print(f'Created storage {storage_name}.')


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
    profile = ctx.parent.obj.get('usercontext')
    if basic_delete(profile, resource_path, resource_name, force_operation=True):
        print(f'Deleted {resource_name}')
    else:
        print(f'Could not delete {resource_name}. Not found.')


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
    profile = ctx.parent.obj.get("usercontext")
    the_value = value
    if value:
        the_value = value
        if (stripped := value.strip()).startswith('[') and stripped.endswith(']'):
            the_value = json.loads(stripped)
    basic_settings(profile, resource_path, key, the_value, force_operation=True)


storage.add_command(command_list)
storage.add_command(create)
storage.add_command(delete)
storage.add_command(command_show)
storage.add_command(settings)
