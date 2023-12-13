"""Commands relative to project handling  operations"""
import json
import click

from ...library_api.common.generic_resource import access_resource

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import (ResourceNotFoundException,
                                              MissingSettingsException,
                                              InvalidFormatFileException)
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.context import ProfileUserContext

from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ..common.misc_operations import settings as command_settings
from ..common.undecorated_click_commands import (basic_create,
                                                 basic_create_with_body_from_string)


@click.group(help="Dictionary-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--dictionary', 'dictionary_name', help="Perform operation on the passed dictionary.",
              metavar='DICTIONARYNAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def dictionary(ctx: click.Context,
               project_name,
               dictionary_name):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      dictionaryname=dictionary_name)

    project = user_profile.projectname
    if not project:
        raise ResourceNotFoundException(
            f"No project parameter provided and "
            f"no project set in profile '{user_profile.profilename}'")

    project_id = access_resource(user_profile,
                                 [('projects', project)])['uuid']
    resource_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/dictionaries/'
    ctx.obj = {'resource_path': resource_path,
               'usercontext': user_profile}


@click.group(help="Files operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def files(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = f'{ctx.obj["resource_path"]}files'
    ctx.obj = {'resource_path': resource_path,
               'usercontext': user_profile}


@click.command(help="Create dictionary. 'dictionary_settings_file' contains the settings of "
                    "the dictionary. The filename and name in settings will be replaced by "
                    "'dictionary_filename' and 'dictionary_name' respectively.")
@click.argument('dictionary_settings_file')
@click.argument('dictionary_filename')
@click.argument('dictionary_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_dict(ctx: click.Context,
                dictionary_settings_file: str,
                dictionary_filename: str,
                dictionary_name: str):
    profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    with open(dictionary_settings_file, 'r', encoding='utf-8') as input_body:
        try:
            dictionary_body = json.loads(input_body.read())
        except json.decoder.JSONDecodeError as exc:
            raise InvalidFormatFileException(
                f"Unexpected data structure found in {dictionary_settings_file}") from exc

    if not dictionary_body.get('settings'):
        raise MissingSettingsException(f"Missing 'settings' field in {dictionary_settings_file}")

    dictionary_body['settings']['filename'] = dictionary_filename
    basic_create_with_body_from_string(profile,
                                       resource_path,
                                       dictionary_name,
                                       json.dumps(dictionary_body))
    print(f'Created {dictionary_name}')


@click.command(help='Upload a dictionary file.')
@click.option('--body-from-file-type', '-t',
              type=click.Choice(('json', 'verbatim')),
              help='How to interpret the body from option. ',
              metavar='BODYFROMFILETYPE',
              default='json')
@click.argument('dictionary_file_to_upload')
@click.argument('dictionary_filename')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def upload_file_dict(ctx: click.Context,
                     dictionary_file_to_upload: str,
                     dictionary_filename,
                     body_from_file_type):
    profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    basic_create(profile, resource_path,
                 dictionary_filename,
                 dictionary_file_to_upload,
                 body_from_file_type)
    print(f'Uploaded dictionary file from {dictionary_file_to_upload} '
          f'with name {dictionary_filename}.')


@click.command(help='Delete dictionary file.')
@click.argument('dictionary_filename')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def dict_file_delete(ctx: click.Context, dictionary_filename):
    profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    resource_url = f'{scheme}://{hostname}{resource_path}/{dictionary_filename}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    rest_ops.delete(resource_url, headers=headers, timeout=timeout)
    print(f'Deleted {dictionary_filename}')


dictionary.add_command(create_dict, name='create')
dictionary.add_command(files)
files.add_command(upload_file_dict, name='upload')
files.add_command(command_list)
files.add_command(dict_file_delete, name='delete')

dictionary.add_command(command_list)
dictionary.add_command(command_delete)
dictionary.add_command(command_show)
dictionary.add_command(command_settings)
