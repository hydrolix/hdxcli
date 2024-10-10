import copy
import io
import json
from urllib.parse import urlparse

from hdx_cli.cli_interface.migrate.helpers import MigrationData
from hdx_cli.library_api.common import rest_operations as lro
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create_with_body_from_string
from hdx_cli.library_api.common.exceptions import HttpException, ResourceNotFoundException, HdxCliException
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_equivalent_storages, get_storage_default, valid_storage_id
from hdx_cli.library_api.common.context import ProfileUserContext

logger = get_logger()


def normalize_summary_table(table: dict) -> dict:
    summary = table.get('settings', {}).get('summary')
    if isinstance(summary, dict) and summary:
        summary.pop('parents', None)
        summary.pop('summary_sql', None)
    return table


def normalize_table(table: dict, reuse_partitions: bool) -> dict:
    for key in ['url', 'created', 'modified', 'project']:
        table.pop(key, None)
    if not reuse_partitions:
        table.pop('uuid', None)

    table.get('settings', {}).pop('autoingest', None)

    merge = table.get('settings', {}).get('merge')
    if isinstance(merge, dict) and merge:
        for key in ['pools', 'sql']:
            merge.pop(key, None)

    if table.get('type') == 'summary':
        table = normalize_summary_table(table)

    return table


def normalize_transform(transform: dict) -> dict:
    for key in ['uuid', 'created', 'modified', 'table', 'url']:
        transform.pop(key, None)

    sample_data = transform.get('settings', {}).get('sample_data')
    if isinstance(sample_data, dict) and not sample_data:
        transform['settings'].pop('sample_data', None)

    return transform


def create_resources(target_profile: ProfileUserContext,
                     target_data: MigrationData,
                     source_profile: ProfileUserContext,
                     source_data: MigrationData,
                     reuse_partitions: bool = False
                     ) -> None:
    logger.info(f'{" Resources ":=^50}')
    logger.info(f"{f'Creating resources in {target_profile.hostname[:27]}':<50}")

    # PROJECT
    _create_project(target_profile, source_data, reuse_partitions)
    target_data.project, target_project_url = access_resource_detailed(
        target_profile,
        [
            ('projects', target_profile.projectname)
        ]
    )
    if reuse_partitions and source_data.project.get('uuid') != target_data.project.get('uuid'):
        raise HdxCliException('The source and target resources must have the same UUID.')

    # FUNCTIONS
    if source_data.functions:
        _create_functions(target_profile, source_data)

    # DICTIONARIES
    if source_data.dictionaries:
        _create_dictionaries(target_profile, source_profile, source_data)

    # TABLE
    _create_table(target_profile, source_data, target_project_url, reuse_partitions)
    target_data.table, target_table_url = access_resource_detailed(
        target_profile,
        [
            ('projects', target_profile.projectname),
            ('tables', target_profile.tablename)
        ]
    )

    # If table type is summary, then there are no transforms to create
    if source_data.transforms and source_data.table.get('type') != 'summary':
        # TRANSFORMS
        _create_transforms(target_profile, source_data, target_table_url)

    logger.info('')


def _create_project(target_profile: ProfileUserContext,
                    source_data: MigrationData,
                    reuse_partitions: bool
                    ) -> None:
    logger.info(f"{f'  Project: {target_profile.projectname[:31]}':<42} -> [!n]")
    _, target_projects_url = access_resource_detailed(target_profile, [('projects', None)])
    target_projects_path = urlparse(f'{target_projects_url}').path

    source_project_body = copy.deepcopy(source_data.project)

    if not reuse_partitions:
        source_project_body.pop('uuid', None)
    try:
        basic_create_with_body_from_string(
            target_profile,
            target_projects_path,
            target_profile.projectname,
            json.dumps(source_project_body)
        )
        logger.info('Done')
    except HttpException as exc:
        if exc.error_code != 400 or 'already exists' not in str(exc.message):
            raise exc
        logger.info('Exists, skipping')


def _create_functions(target_profile: ProfileUserContext,
                      source_data: MigrationData,
                      ) -> None:
    logger.info(f"{f'  Functions':<42} -> [!n]")
    _, target_project_url = access_resource_detailed(
        target_profile,
        [("projects", target_profile.projectname)]
    )
    target_functions_path = urlparse(f"{target_project_url}functions/").path
    error_flag = False
    for function in source_data.functions:
        del function["uuid"]
        function_name = function.get("name")
        try:
            basic_create_with_body_from_string(
                target_profile,
                target_functions_path,
                function_name,
                json.dumps({"sql": function["sql"]})
            )
        except HttpException as exc:
            if exc.error_code != 400 or "already exists" not in str(exc.message):
                logger.debug(f"Error creating function '{function_name}': {exc}")
                error_flag = True
            else:
                logger.debug(f"Function '{function_name}' already exists, skipping")
            continue
    message = "Done with errors" if error_flag else "Done"
    logger.info(message)


def _create_dictionaries(target_profile: ProfileUserContext,
                         source_profile: ProfileUserContext,
                         source_data: MigrationData
                         ) -> None:
    logger.info(f"{f'  Dictionaries':<42} -> [!n]")
    _, target_project_url = access_resource_detailed(
        target_profile,
        [("projects", target_profile.projectname)]
    )
    target_project_path = urlparse(target_project_url).path
    target_dict_path = f"{target_project_path}dictionaries/"
    dictionary_files_so_far = set()
    error_flag = False
    for dic in source_data.dictionaries:
        d_settings = dic["settings"]
        d_name = dic["name"]
        d_file = d_settings["filename"]
        d_format = d_settings["format"]
        table_name = f"{source_profile.projectname}_{d_name}"
        query_endpoint = (f"{source_profile.scheme}://{source_profile.hostname}"
                          f"/query/?query=SELECT * FROM {table_name} FORMAT {d_format}")
        headers = {"Authorization": f"{source_profile.auth.token_type} {source_profile.auth.token}",
                   "Accept": "*/*"}
        timeout = source_profile.timeout
        contents = lro.get(query_endpoint, headers=headers, timeout=timeout, fmt="verbatim")
        try:
            if d_file not in dictionary_files_so_far:
                _create_dictionary_file(
                    target_profile.projectname,
                    d_file,
                    contents,
                    target_profile
                )
                dictionary_files_so_far.add(d_file)
        except HttpException as exc:
            if exc.error_code != 400 or "already exists" not in str(exc.message):
                logger.debug(f"Error creating dictionary file '{d_file}': {exc}")
                error_flag = True
            else:
                logger.debug(f"Dictionary file '{d_file}' already exists, skipping")
            continue
        finally:
            try:
                basic_create_with_body_from_string(
                    target_profile,
                    target_dict_path,
                    d_name,
                    json.dumps({'settings': d_settings})
                )
            except HttpException as exc:
                if exc.error_code != 400 or "already exists" not in str(exc.message):
                    logger.debug(f"Error creating dictionary file '{d_file}': {exc}")
                    error_flag = True
                else:
                    logger.debug(f"Dictionary '{d_name}' already exists, skipping")

    message = "Done with errors" if error_flag else "Done"
    logger.info(message)


def _create_dictionary_file(project_name: str,
                            dict_file: str,
                            contents,
                            profile: ProfileUserContext
                            ) -> None:
    _, project_url = access_resource_detailed(profile, [('projects', project_name)])
    headers = {'Authorization': f'{profile.auth.token_type} {profile.auth.token}',
               'Accept': '*/*'}
    file_url = f'{project_url}dictionaries/files/'
    timeout = profile.timeout
    lro.create_file(
        file_url,
        headers=headers,
        file_stream=io.BytesIO(contents),
        remote_filename=dict_file,
        timeout=timeout
    )


def _create_table(target_profile: ProfileUserContext,
                  source_data: MigrationData,
                  target_project_url: str,
                  reuse_partitions: bool
                  ) -> None:
    logger.info(f"{f'  Table: {target_profile.tablename[:33]}':<42} -> [!n]")
    target_tables_path = urlparse(f'{target_project_url}tables/').path
    table_body = copy.deepcopy(source_data.table)

    normalized_table = normalize_table(table_body, reuse_partitions)
    basic_create_with_body_from_string(
        target_profile,
        target_tables_path,
        target_profile.tablename,
        json.dumps(normalized_table)
    )
    logger.info('Done')


def _create_transforms(target_profile: ProfileUserContext,
                       source_data: MigrationData,
                       target_table_url: str
                       ) -> None:
    logger.info(f"{f'  Transforms':<42} -> [!n]")
    target_transforms_path = urlparse(f'{target_table_url}transforms/').path
    for transform in source_data.transforms:
        normalized_transform = normalize_transform(transform)
        transform_name = normalized_transform.get('name')
        basic_create_with_body_from_string(
            target_profile,
            target_transforms_path,
            transform_name,
            json.dumps(normalized_transform)
        )
    logger.info('Done')


def get_resources(profile: ProfileUserContext,
                  data: MigrationData,
                  only_storages: bool = False
                  ) -> None:
    logger.info(f"{f'Getting resources from {profile.hostname[:27]}':<50}")

    if not only_storages:
        logger.info(f"{f'  Project: {profile.projectname[:31]}':<42} -> [!n]")
        data.project, _ = access_resource_detailed(profile, [('projects', profile.projectname)])
        if not data.project:
            raise ResourceNotFoundException(f"The project '{profile.projectname}' was not found.")
        logger.info('Done')

        logger.info(f"{f'  Functions':<42} -> [!n]")
        data.functions, _ = access_resource_detailed(profile,
                                                     [('projects', profile.projectname),
                                                    ('functions', None)])
        logger.info("Done")

        logger.info(f"{f'  Dictionaries':<42} -> [!n]")
        data.dictionaries, _ = access_resource_detailed(profile,
                                                        [('projects', profile.projectname),
                                                        ('dictionaries', None)])
        logger.info("Done")

        logger.info(f"{f'  Table: {profile.tablename[:33]}':<42} -> [!n]")
        data.table, _ = access_resource_detailed(
            profile,
            [
                ('projects', profile.projectname),
                ('tables', profile.tablename)
            ]
        )
        if not data.table:
            raise ResourceNotFoundException(f"The table '{profile.tablename}' was not found.")
        logger.info('Done')

        logger.info(f"{'  Transforms':<42} -> [!n]")
        data.transforms, _ = access_resource_detailed(
            profile,
            [
                ('projects', profile.projectname),
                ('tables', profile.tablename),
                ('transforms', None)
            ]
        )
        if not data.transforms:
            raise ResourceNotFoundException(
                f"Transforms in the table '{profile.tablename}' were not found."
            )
        logger.info('Done')

    logger.info(f"{'  Storages':<42} -> [!n]")
    data.storages, _ = access_resource_detailed(profile, [('storages', None)])
    logger.info('Done')


def update_equivalent_multi_storage_settings(source_data: MigrationData,
                                             target_storages: list[dict]
                                             ) -> None:
    storage_equivalences = get_equivalent_storages(source_data.storages, target_storages)
    table_body = source_data.table
    storage_map = table_body.get('settings').get('storage_map')
    default_storage_id = storage_map.get('default_storage_id')
    if not (new_default_storage_id := storage_equivalences.get(default_storage_id)):
        raise HdxCliException(
            f"Storage ID '{default_storage_id}' not found in the target storages."
        )

    storage_map['default_storage_id'] = new_default_storage_id

    if mapping := storage_map.get('column_value_mapping'):
        new_mapping = {}
        for storage, values in mapping.items():
            if not (new_storage := storage_equivalences.get(default_storage_id)):
                raise HdxCliException(
                    f"Storage ID '{default_storage_id}' not found in the target storages."
                )
            new_mapping[new_storage] = values
        storage_map['column_value_mapping'] = new_mapping


def interactive_set_default_storage(source_data: MigrationData,
                                   target_storages: list[dict]
                                   ) -> None:
    logger.info('')
    table_body = source_data.table
    default_storage_id, _ = get_storage_default(target_storages)

    logger.info(f'{" Default Storage Settings ":-^50}')
    logger.info('Specify the storage UUID for the new table, or')
    logger.info(f'press Enter to use the cluster default storage.')

    for attempt in range(3):
        logger.info('')
        logger.info(f'Default storage UUID ({default_storage_id}): [!i]')
        user_input = input().strip().lower()

        if not user_input:
            break
        elif valid_storage_id(user_input, target_storages):
            default_storage_id = user_input
            break
        else:
            logger.info('Invalid storage UUID. Please try again.')
    else:
        raise HdxCliException("Storage UUID not found in the target cluster.")

    table_settings = table_body.get('settings')
    table_settings['storage_map'] = {'default_storage_id': default_storage_id}

    logger.info(f'{"  ":-^50}')
    logger.info(f"{'  Updating default storage settings':<42} -> [!n]")
