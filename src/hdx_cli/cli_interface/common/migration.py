import os
import io
from pathlib import Path

import tempfile
from urllib.parse import urlparse
import toml

from ...library_api.common.auth_utils import load_user_context
from ...library_api.common.context import ProfileLoadContext, ProfileUserContext
from ...library_api.common.exceptions import HttpException
from ...library_api.common import rest_operations as lro
from ...library_api.common.login import login
from ...library_api.common.auth import load_profile, save_profile_cache
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.config_constants import PROFILE_CONFIG_FILE
from ..common.undecorated_click_commands import basic_create_from_dict_body


def _setup_target_cluster_config(profile_config_file,
                                 target_cluster_username,
                                 target_cluster_hostname,
                                 target_cluster_scheme):
    username = target_cluster_username
    hostname = target_cluster_hostname
    scheme = target_cluster_scheme
    config_data = {'default': {'username': username, 'hostname': hostname, 'scheme': scheme}}
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(config_data, config_file)


def generate_target_profile(target_cluster_hostname,
                            target_cluster_username,
                            target_cluster_password,
                            target_cluster_uri_scheme):
    target_profiles_file = Path(tempfile.gettempdir() + os.sep +
                                target_cluster_username + '_' +
                                target_cluster_hostname + '.toml')
    _setup_target_cluster_config(target_profiles_file,
                                 target_cluster_username,
                                 target_cluster_hostname,
                                 target_cluster_uri_scheme)
    target_load_ctx = ProfileLoadContext('default', target_profiles_file)
    auth_info = login(target_cluster_username,
                      target_cluster_hostname,
                      password=target_cluster_password,
                      use_ssl=(target_cluster_uri_scheme == 'https'))
    target_profile = load_profile(target_load_ctx)
    target_profile.auth = auth_info
    target_profile.org_id = auth_info.org_id

    save_profile_cache(target_profile,
                       token=target_profile.auth.token,
                       org_id=target_profile.org_id,
                       token_type='Bearer',
                       expiration_time=target_profile.auth.expires_at,
                       cache_dir_path=target_profile.profile_config_file.parent)
    return target_profile


def migrate_a_project(source_profile,
                      project_name,
                      target_profile_name,
                      target_cluster_hostname,
                      target_cluster_username,
                      target_cluster_password,
                      target_cluster_uri_scheme):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_project_body, _ = access_resource_detailed(source_profile, [('projects', project_name)])
    _, target_projects_url = access_resource_detailed(target_profile, [('projects', None)])
    target_projects_path = urlparse(target_projects_url).path

    # Deleting invalid keys for table creation.
    remove_keys(source_project_body, 'uuid')
    # Creating resource on target cluster.
    basic_create_from_dict_body(target_profile, target_projects_path, source_project_body)


def migrate_a_table(source_profile,
                    table_name,
                    target_profile_name,
                    target_cluster_hostname,
                    target_cluster_username,
                    target_cluster_password,
                    target_cluster_uri_scheme,
                    target_project_name):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_table_body, _ = access_resource_detailed(source_profile,
                                                    [('projects', source_profile.projectname),
                                                     ('tables', table_name)])
    _, target_tables_url = access_resource_detailed(target_profile,
                                                    [('projects', target_project_name),
                                                     ('tables', None)])
    target_tables_path = urlparse(target_tables_url).path

    remove_keys(source_table_body, 'uuid', ('settings', 'autoingest', 0, 'source'))
    basic_create_from_dict_body(target_profile, target_tables_path, source_table_body)


def migrate_a_transform(source_profile,
                        transform_name,
                        target_profile_name,
                        target_cluster_hostname,
                        target_cluster_username,
                        target_cluster_password,
                        target_cluster_uri_scheme,
                        target_project_name,
                        target_table_name):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_transform_body, _ = access_resource_detailed(source_profile,
                                                        [('projects', source_profile.projectname),
                                                         ('tables', source_profile.tablename),
                                                         ('transforms', transform_name)])
    _, target_transforms_url = access_resource_detailed(target_profile,
                                                        [('projects', target_project_name),
                                                         ('tables', target_table_name),
                                                         ('transforms', None)])
    target_transforms_path = urlparse(target_transforms_url).path

    remove_keys(source_transform_body, 'uuid')
    basic_create_from_dict_body(target_profile, target_transforms_path, source_transform_body)


def migrate_a_dictionary(source_profile,
                         dictionary_name,
                         target_profile_name,
                         target_cluster_hostname,
                         target_cluster_username,
                         target_cluster_password,
                         target_cluster_uri_scheme,
                         target_project_name):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_dictionaries_body, _ = access_resource_detailed(source_profile,
                                                           [('projects', source_profile.projectname),
                                                            ('dictionaries', dictionary_name)])
    _, target_dictionaries_url = access_resource_detailed(target_profile,
                                                          [('projects', target_project_name),
                                                           ('dictionaries', None)])
    target_dictionaries_path = urlparse(target_dictionaries_url).path

    _migrate_dict_file(source_profile, target_profile, source_dictionaries_body,
                       source_profile.projectname, target_project_name)

    remove_keys(source_dictionaries_body, 'uuid')
    basic_create_from_dict_body(target_profile, target_dictionaries_path, source_dictionaries_body)


def migrate_a_function(source_profile,
                       function_name,
                       target_profile_name,
                       target_cluster_hostname,
                       target_cluster_username,
                       target_cluster_password,
                       target_cluster_uri_scheme,
                       target_project_name):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_function_body, _ = access_resource_detailed(source_profile,
                                                       [('projects', source_profile.projectname),
                                                        ('functions', function_name)])
    _, target_functions_url = access_resource_detailed(target_profile,
                                                       [('projects', target_project_name),
                                                        ('functions', None)])
    target_functions_path = urlparse(target_functions_url).path

    remove_keys(source_function_body, 'uuid')
    basic_create_from_dict_body(target_profile, target_functions_path, source_function_body)


def migrate_a_storage(source_profile,
                      storage_name,
                      target_profile_name,
                      target_cluster_hostname,
                      target_cluster_username,
                      target_cluster_password,
                      target_cluster_uri_scheme):
    target_profile = get_target_profile(target_profile_name,
                                        target_cluster_hostname,
                                        target_cluster_username,
                                        target_cluster_password,
                                        target_cluster_uri_scheme,
                                        source_profile.timeout)

    source_storages_body, _ = access_resource_detailed(source_profile,
                                                       [('storages', storage_name)])
    _, target_storages_url = access_resource_detailed(target_profile,
                                                      [('storages', None)])
    target_storages_path = urlparse(target_storages_url).path

    remove_keys(source_storages_body, 'uuid')
    basic_create_from_dict_body(target_profile, target_storages_path, source_storages_body)


def get_target_profile(target_profile_name,
                       target_cluster_hostname,
                       target_cluster_username,
                       target_cluster_password,
                       target_cluster_uri_scheme,
                       target_timeout):
    if target_profile_name:
        load_context = ProfileLoadContext(target_profile_name, PROFILE_CONFIG_FILE)
        target_profile = load_user_context(load_context)
    else:
        target_profile = generate_target_profile(target_cluster_hostname,
                                                 target_cluster_username,
                                                 target_cluster_password,
                                                 target_cluster_uri_scheme)
    target_profile.timeout = target_timeout
    return target_profile


def _migrate_dict_file(source_profile, target_profile, dictionary, source_project_name, target_project_name):
    d_settings = dictionary['settings']
    d_name = dictionary['name']
    d_file = d_settings['filename']
    d_format = d_settings['format']
    table_name = f'{source_project_name}_{d_name}'
    query_endpoint = (f'{source_profile.scheme}://{source_profile.hostname}'
                      f'/query/?query=SELECT * FROM {table_name} FORMAT {d_format}')
    headers = {'Authorization': f'{source_profile.auth.token_type} {source_profile.auth.token}',
               'Accept': '*/*'}
    timeout = source_profile.timeout
    contents = lro.get(query_endpoint, headers=headers, timeout=timeout, fmt='verbatim')
    try:
        _create_dictionary_file_for_project(target_project_name, d_file, contents,
                                            target_profile)
    except HttpException:
        # Dictionary file existed, no need to create it
        pass


def _create_dictionary_file_for_project(project_name,
                                        dict_file,
                                        contents,
                                        profile: ProfileUserContext):
    _, project_url = access_resource_detailed(profile,
                                              [('projects', project_name)])

    headers = {'Authorization': f'{profile.auth.token_type} {profile.auth.token}',
               'Accept': '*/*'}
    file_url = f'{project_url}dictionaries/files/'
    timeout = profile.timeout
    lro.create_file(file_url, headers=headers,
                    file_stream=io.BytesIO(contents),
                    remote_filename=dict_file,
                    timeout=timeout)


def remove_keys(resource, *args):
    for key in args:
        if isinstance(key, tuple):
            current_level = resource
            for nested_key in key[:-1]:
                if (isinstance(current_level, list) and isinstance(nested_key, int)
                        and 0 <= nested_key < len(current_level)):
                    current_level = current_level[nested_key]
                elif nested_key in current_level:
                    current_level = current_level[nested_key]
                else:
                    # If any of the nested keys doesn't exist, simply return without doing anything.
                    return

            last_key = key[-1]
            if (isinstance(current_level, list) and isinstance(last_key, int)
                    and 0 <= last_key < len(current_level)):
                del current_level[last_key]
            elif last_key in current_level:
                del current_level[last_key]
        else:
            if key in resource:
                del resource[key]
