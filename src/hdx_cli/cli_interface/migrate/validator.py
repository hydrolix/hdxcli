from datetime import datetime

from hdx_cli.library_api.common.exceptions import HdxCliException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_equivalent_storages

from ...models import ProfileUserContext
from ..common.cached_operations import find_alter_jobs
from .catalog_operations import Catalog
from .helpers import MigrationData
from .resources import interactive_set_default_storage, update_equivalent_multi_storage_settings

logger = get_logger()


def validations(
    source_profile: ProfileUserContext,
    source_data: MigrationData,
    target_data: MigrationData,
    catalog: Catalog,
    from_date: datetime,
    to_date: datetime,
    only: str,
    allow_merge: bool,
    reuse_partitions: bool,
) -> None:
    logger.info(f'{" Validation ":=^50}')
    table_constraints(source_profile, source_data.table, only, allow_merge)
    filter_catalog(catalog, from_date, to_date)
    validate_reuse_partitions(source_data, target_data, catalog, only, reuse_partitions)
    validate_multi_bucket(source_data, target_data.storages, only, reuse_partitions)


def check_merge_table_settings(
    profile: ProfileUserContext, table_body: dict, is_only_resources: bool, allow_merge=False
) -> None:
    logger.info(f"{'Checking merge table settings':<42} -> [!n]")
    if not allow_merge and not is_only_resources and is_merge_enable(table_body):
        raise HdxCliException(
            f"The merging process is enabled in the '{profile.tablename}' table. "
            "To successfully migrate data, be sure to disable it or use --allow-merge "
            "to skip this validation."
        )
    message = "Skipped "
    message += "(--only resources)" if is_only_resources else "(--allow-merge)"
    if not allow_merge and not is_only_resources:
        message = "Done"
    logger.info(message)


def check_alter_jobs(profile: ProfileUserContext, is_only_resources: bool) -> None:
    logger.info(f"{'Checking for running alter jobs':<42} -> [!n]")
    if is_only_resources:
        logger.info("Skipped (--only resources)")
        return

    alter_jobs = find_alter_jobs(profile)
    is_alter_job_running = list(
        filter(
            lambda x: x.get("status") == "running"
            and x.get("settings", {}).get("project_name") == profile.projectname
            and x.get("settings", {}).get("table_name") == profile.tablename,
            alter_jobs,
        )
    )
    if is_alter_job_running:
        raise HdxCliException(
            f"There is an alter job running on the '{profile.tablename}' table. "
            "To successfully migrate data, be sure there are not alter job processes "
            "running on this table."
        )
    logger.info("Done")


def table_constraints(
    profile: ProfileUserContext, table_body: dict, only: str, allow_merge
) -> None:
    is_only_resources = only == "resources"
    check_merge_table_settings(profile, table_body, is_only_resources, allow_merge)
    check_alter_jobs(profile, is_only_resources)


def is_merge_enable(table_body: dict) -> bool:
    return table_body.get("settings", {}).get("merge", {}).get("enabled")


def has_multi_buckets(table_body: dict) -> bool:
    return table_body.get("settings", {}).get("storage_map", {}).get("default_storage_id")


def is_same_uuid(source_resource: dict, target_resource: dict) -> bool:
    source_uuid = source_resource.get("uuid")
    target_uuid = target_resource.get("uuid")

    if source_uuid is None or target_uuid is None:
        return False
    return source_uuid == target_uuid


def filter_catalog(
    catalog: Catalog, from_date: datetime = False, to_date: datetime = False
) -> None:
    if not catalog or not (from_date or to_date):
        return

    logger.info(f"{'Filtering catalog by timestamp':<42} -> [!n]")
    catalog.filter_by_timestamp(from_date, to_date)
    logger.info("Done")


def validate_reuse_partitions(
    source_data: MigrationData,
    target_data: MigrationData,
    catalog: Catalog,
    only: str,
    reuse_partitions: bool = False,
) -> None:
    if not reuse_partitions:
        return

    if only != "resources":
        logger.info(f"{'Updating catalog (--reuse-partitions)':<42} -> [!n]")
        storage_equivalences = get_equivalent_storages(source_data.storages, target_data.storages)
        catalog.update_with_shared_storages(storage_equivalences)
        logger.info("Done")

    if only == "data":
        logger.info(f"{'Checking UUIDs (--reuse-partitions)':<42} -> [!n]")
        if not is_same_uuid(source_data.project, target_data.project) or not is_same_uuid(
            source_data.table, target_data.table
        ):
            raise HdxCliException("The source and target resources must have the same UUID.")
        logger.info("Done")


def validate_multi_bucket(
    source_data: MigrationData,
    target_storages: list[dict],
    only: str,
    reuse_partitions: bool = False,
) -> None:
    if only == "data":
        return

    logger.info(f"{'Verifying table storage settings':<42} -> [!n]")
    if has_multi_buckets(source_data.table) and reuse_partitions:
        update_equivalent_multi_storage_settings(source_data, target_storages)
    else:
        interactive_set_default_storage(source_data.table, target_storages)
    logger.info("Done")
