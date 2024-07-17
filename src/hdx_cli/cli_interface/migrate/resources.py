import copy
import json
from urllib.parse import urlparse

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create_with_body_from_string
from hdx_cli.library_api.common.exceptions import HttpException, ResourceNotFoundException
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


def creating_resources(target_profile, target_data, source_data, reuse_partitions: bool = False) -> None:
    logger.info(f'{" Resources ":=^50}')
    logger.info(f'Creating resources in {target_profile.hostname}')

    # PROJECT
    logger.info(f"{f'  Project: {target_profile.projectname}':<42} -> [!n]")
    _, target_projects_url = access_resource_detailed(target_profile, [('projects', None)])
    target_projects_path = urlparse(f'{target_projects_url}').path

    source_project_body = copy.deepcopy(source_data.project)
    try:
        if not reuse_partitions:
            del source_project_body['uuid']
        basic_create_with_body_from_string(target_profile,
                                           target_projects_path,
                                           target_profile.projectname,
                                           json.dumps(source_project_body))
        logger.info('Done')
    except HttpException as exc:
        if exc.error_code != 400 or 'already exists' not in str(exc.message):
            raise exc
        logger.info('Exists, skipping')

    # Getting project body and project_url of the already created project
    target_data.project, target_project_url = access_resource_detailed(target_profile,
                                                                       [('projects', target_profile.projectname)])

    # TABLE
    logger.info(f"{f'  Table: {target_profile.tablename}':<42} -> [!n]")
    target_tables_path = urlparse(f'{target_project_url}tables/').path
    table_body = copy.deepcopy(source_data.table)
    if not reuse_partitions:
        del table_body['uuid']
    basic_create_with_body_from_string(target_profile,
                                       target_tables_path,
                                       target_profile.tablename,
                                       json.dumps(table_body))
    logger.info('Done')

    # Getting table body and table_url of the already created table
    target_data.table, target_table_url = access_resource_detailed(target_profile,
                                                                   [('projects', target_profile.projectname),
                                                                    ('tables', target_profile.tablename)])

    # TRANSFORMS
    transform_names = ','.join(list(map(lambda t: t['name'], source_data.transforms)))
    logger.info(f"{f'  Transforms: {transform_names}':<42} -> [!n]")
    target_transforms_path = urlparse(f'{target_table_url}transforms/').path
    for transform in source_data.transforms:
        del transform['uuid']
        transform_name = transform.get('name')
        basic_create_with_body_from_string(target_profile,
                                           target_transforms_path,
                                           transform_name,
                                           json.dumps(transform))
    logger.info('Done')
    logger.info('')


def get_resources(profile, data, only_storages=False) -> None:
    logger.info(f"{f'Getting resources from {profile.hostname}':<50}")

    if not only_storages:
        logger.info(f'{f"  Project: {profile.projectname}":<42} -> [!n]')
        data.project, _ = access_resource_detailed(profile, [('projects', profile.projectname)])
        if not data.project:
            raise ResourceNotFoundException(f"The project '{profile.projectname}' was not found.")
        logger.info('Done')

        logger.info(f"{f'  Table: {profile.tablename}':<42} -> [!n]")
        data.table, _ = access_resource_detailed(profile, [('projects', profile.projectname),
                                                           ('tables', profile.tablename)])
        if not data.table:
            raise ResourceNotFoundException(f"The table '{profile.tablename}' was not found.")
        logger.info('Done')

        logger.info(f"{'  Transforms: '}[!n]")
        data.transforms, _ = access_resource_detailed(profile, [('projects', profile.projectname),
                                                                ('tables', profile.tablename),
                                                                ('transforms', None)])
        if not data.transforms:
            raise ResourceNotFoundException(
                f"Transforms in the table '{profile.tablename}' were not found.")
        transform_names = ','.join(list(map(lambda t: t['name'], data.transforms)))
        logger.info(f'{transform_names:<28} -> [!n]')
        logger.info('Done')

    # Then, storages
    logger.info(f"{'  Storages: ':}[!n]")
    data.storages, _ = access_resource_detailed(profile, [('storages', None)])
    storage_names = ','.join(list(map(lambda t: t['name'], data.storages)))
    logger.info(f'{storage_names:<30} -> [!n]')
    logger.info('Done')
