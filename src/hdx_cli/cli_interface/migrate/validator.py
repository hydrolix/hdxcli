from datetime import datetime

from hdx_cli.cli_interface.common.undecorated_click_commands import get_resource_list
from hdx_cli.cli_interface.migrate.helpers import MigrationData
from hdx_cli.library_api.common.catalog import Catalog
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.storage import equivalent_storages
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


def validates(source_profile: ProfileUserContext, source_data: MigrationData,
              target_data: MigrationData, catalog: Catalog, min_timestamp: datetime,
              max_timestamp: datetime, allow_merge: bool, reuse_partitions: bool) -> None:
    logger.info("Running some validations")
    table_constraints(source_profile, source_data.table, allow_merge)
    filter_catalog(catalog, min_timestamp, max_timestamp)
    validate_reuse_partitions(source_data.storages, target_data.storages, catalog, reuse_partitions)


def table_constraints(profile: ProfileUserContext, table_body: dict, allow_merge=False) -> None:
    # Validate if 'merge' is not enabled in the settings table
    # Or if the user passed the '--allow-merge' option
    logger.info(f"{'  Checking merge table settings':<42} -> [!n]")
    if not allow_merge and is_merge_enable(table_body):
        raise Exception(f"The merging process is enabled in the '{profile.tablename}' table. "
                        "To successfully migrate data, be sure to disable it or use --allow-merge "
                        "to skip this validation.")
    logger.info(f'{"Done" if not allow_merge else "Skip due to --allow-merge"}')

    # Validate if the table to be migrated has an alter job in a running state
    logger.info(f"{'  Looking for running alter jobs':<42} -> [!n]")
    alter_path = f'/config/v1/orgs/{profile.org_id}/jobs/alter/'
    alter_jobs = get_resource_list(profile, alter_path).get('results')
    is_alter_job_running = list(filter(
        lambda x: x.get('status') == 'running' and
                  x.get('settings', {}).get('project_name') == profile.projectname and
                  x.get('settings', {}).get('table_name') == profile.tablename,
        alter_jobs))
    if is_alter_job_running:
        raise Exception(f"There is an alter job running on the '{profile.tablename}' table. "
                        "To successfully migrate data, be sure there are not alter job processes "
                        "running on this table.")
    logger.info('Done')


def is_merge_enable(table_body: dict) -> bool:
    return table_body.get('settings', {}).get('merge', {}).get('enabled')


def filter_catalog(catalog: Catalog, min_timestamp: datetime = False,
                   max_timestamp: datetime = False) -> None:
    if not (min_timestamp or max_timestamp):
        return

    logger.info(f"{'  Filtering catalog by timestamp':<42} -> [!n]")
    catalog.filter_by_timestamp(min_timestamp, max_timestamp)
    logger.info('Done')


def validate_reuse_partitions(source_storages: list[dict], target_storages: list[dict],
                              catalog: Catalog, reuse_partitions: bool = False) -> None:
    if not reuse_partitions:
        return

    logger.info(f"{'  Updating catalog':<42} -> [!n]")
    storage_equivalences = equivalent_storages(source_storages, target_storages)
    catalog.update_equivalent_storage(storage_equivalences)
    logger.info('Done')
