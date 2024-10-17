from datetime import datetime

from .helpers import MigrationData
from .resources import (
    update_equivalent_multi_storage_settings,
    interactive_set_default_storage
)
from .catalog_operations import Catalog
from hdx_cli.cli_interface.common.undecorated_click_commands import get_resource_list
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.storage import get_equivalent_storages
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.exceptions import HdxCliException

logger = get_logger()


def validations(source_profile: ProfileUserContext,
                source_data: MigrationData,
                target_data: MigrationData,
                catalog: Catalog,
                from_date: datetime,
                to_date: datetime,
                only: str,
                allow_merge: bool,
                reuse_partitions: bool
                ) -> None:
    logger.info("Running some validations")
    table_constraints(source_profile, source_data.table, only, allow_merge)
    filter_catalog(catalog, from_date, to_date)
    validate_reuse_partitions(source_data, target_data, catalog, only, reuse_partitions)
    validate_multi_bucket(source_data, target_data.storages, only, reuse_partitions)


def table_constraints(profile: ProfileUserContext,
                      table_body: dict,
                      only: str,
                      allow_merge=False
                      ) -> None:
    if only == 'resources':
        return

    logger.info(f"{'  Checking merge table settings':<42} -> [!n]")
    if not allow_merge and is_merge_enable(table_body):
        raise HdxCliException(
            f"The merging process is enabled in the '{profile.tablename}' table. "
            'To successfully migrate data, be sure to disable it or use --allow-merge '
            'to skip this validation.'
        )
    logger.info(f'{"Done" if not allow_merge else "Skip due to --allow-merge"}')

    logger.info(f"{'  Looking for running alter jobs':<42} -> [!n]")
    alter_path = f'/config/v1/orgs/{profile.org_id}/jobs/alter/'
    alter_jobs = get_resource_list(profile, alter_path).get('results')
    is_alter_job_running = list(filter(
        lambda x: x.get('status') == 'running' and
                  x.get('settings', {}).get('project_name') == profile.projectname and
                  x.get('settings', {}).get('table_name') == profile.tablename,
        alter_jobs))
    if is_alter_job_running:
        raise HdxCliException(
            f"There is an alter job running on the '{profile.tablename}' table. "
            'To successfully migrate data, be sure there are not alter job processes '
            'running on this table.'
        )
    logger.info('Done')


def is_merge_enable(table_body: dict) -> bool:
    return table_body.get('settings', {}).get('merge', {}).get('enabled')


def has_multi_buckets(table_body: dict) -> bool:
    return table_body.get('settings', {}).get('storage_map', {}).get('default_storage_id')


def is_same_uuid(source_resource: dict, target_resource: dict) -> bool:
    source_uuid = source_resource.get('uuid')
    target_uuid = target_resource.get('uuid')

    if source_uuid is None or target_uuid is None:
        return False
    return source_uuid == target_uuid


def filter_catalog(catalog: Catalog,
                   from_date: datetime = False,
                   to_date: datetime = False
                   ) -> None:
    if not catalog or not (from_date or to_date):
        return

    logger.info(f"{'  Filtering catalog by timestamp':<42} -> [!n]")
    catalog.filter_by_timestamp(from_date, to_date)
    logger.info('Done')


def validate_reuse_partitions(source_data: MigrationData,
                              target_data: MigrationData,
                              catalog: Catalog,
                              only: str,
                              reuse_partitions: bool = False
                              ) -> None:
    if not reuse_partitions:
        return

    if only != 'resources':
        logger.info(f"{'  Updating catalog':<42} -> [!n]")
        storage_equivalences = get_equivalent_storages(source_data.storages, target_data.storages)
        catalog.update_with_shared_storages(storage_equivalences)
        logger.info('Done')

    if only == 'data':
        logger.info(f"{'  Checking resources UUID':<42} -> [!n]")
        if (
                not is_same_uuid(source_data.project, target_data.project) or
                not is_same_uuid(source_data.table, target_data.table)
        ):
            raise HdxCliException('The source and target resources must have the same UUID.')
        logger.info('Done')


def validate_multi_bucket(source_data: MigrationData,
                          target_storages: list[dict],
                          only: str,
                          reuse_partitions: bool = False
                          ) -> None:
    if only == 'data':
        return

    logger.info(f"{'  Checking multi-bucket settings':<42} -> [!n]")
    if has_multi_buckets(source_data.table) and reuse_partitions:
        update_equivalent_multi_storage_settings(source_data, target_storages)
    else:
        interactive_set_default_storage(source_data, target_storages)
    logger.info('Done')
