import io

from urllib.parse import urlparse

from ...library_api.common.auth_utils import load_user_context, generate_temporal_profile
from ...library_api.common.context import ProfileLoadContext, ProfileUserContext
from ...library_api.common.exceptions import HttpException
from ...library_api.common import rest_operations as lro
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.config_constants import PROFILE_CONFIG_FILE
from ..common.undecorated_click_commands import basic_create_from_dict_body


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
    try:
        del source_project_body['uuid']
    except KeyError:
        pass
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

    try:
        del source_table_body['uuid']
        del source_table_body['settings']['autoingest'][0]['source']
    except KeyError:
        pass
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

    try:
        del source_transform_body['uuid']
    except KeyError:
        pass
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

    try:
        del source_dictionaries_body['uuid']
    except KeyError:
        pass
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

    try:
        del source_function_body['uuid']
    except KeyError:
        pass
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

    try:
        del source_storages_body['uuid']
    except KeyError:
        pass
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
        target_profile = generate_temporal_profile(target_cluster_hostname,
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
