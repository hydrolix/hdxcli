import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from .helpers import (
    print_summary,
    confirm_action,
    MigrationData,
    update_catalog_and_upload,
    monitor_progress
)
from hdx_cli.cli_interface.migrate.rc.rc_remotes import RCloneRemote
from hdx_cli.cli_interface.migrate.rc.rc_utils import get_remote, close_remotes, recreate_remotes
from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.cli_interface.migrate.catalog_operations import Catalog
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_storage_default_by_table
from hdx_cli.library_api.common.rest_operations import post_with_retries
from hdx_cli.library_api.common.exceptions import MigrationFailureException

logger = get_logger()


def show_and_confirm_data_migration(catalog: Catalog, reuse_partitions=False) -> bool:
    """
    Displays a summary of the migration process,
    validates that the number of files to migrate is greater than 0,
    and asks for user confirmation to start the migration process.
    """
    total_rows, total_partitions, total_size = catalog.get_summary_information()
    print_summary(total_rows, total_partitions, total_size)

    if reuse_partitions:
        logger.info("The catalog will be uploaded without migrating the data.")

    return confirm_action()


def summary_and_confirm(catalog: Catalog, reuse_partitions=False, remotes=None) -> None:
    if not show_and_confirm_data_migration(catalog, reuse_partitions):
        if remotes:
            close_remotes(remotes)

        logger.info('')
        logger.info(f'{" Migration Finished ":=^50}')
        sys.exit(0)


def migrate_partitions_threaded(migration_list: list,
                                migrated_sizes_queue: Queue,
                                exceptions: Queue,
                                rc_config: RcloneAPIConfig,
                                concurrency: int,
                                remotes: dict
                                ) -> None:
    base_url = rc_config.get_url()
    url = f"{base_url}/sync/copy"

    failed_items = Queue()
    total_items = len(migration_list)
    max_failures = int(total_items * 0.10)

    migration_done = threading.Event()

    def sync_partition(from_to_path):
        if migration_done.is_set():
            return

        # If the migration process has failed more than 10% of the total items, stop the migration process
        failed_count_ = failed_items.qsize()
        if failed_count_ > max_failures:
            migration_done.set()
            exceptions.put(MigrationFailureException(
                    f"Number of failed migrations ({failed_count_}) exceeds "
                    f"the allowed maximum ({max_failures})."
                )
            )
            return

        data = {"srcFs": from_to_path[0], "dstFs": from_to_path[1]}
        response = post_with_retries(url, data, user=rc_config.user, password=rc_config.password)
        if not response or response.status_code != 200:
            failed_items.put(from_to_path)
        else:
            migrated_sizes_queue.put(from_to_path[2])

    def sync_partition_retry(from_to_path):
        if migration_done.is_set():
            return

        data = {"srcFs": from_to_path[0], "dstFs": from_to_path[1]}
        response = post_with_retries(url, data, user=rc_config.user, password=rc_config.password)
        if not response or response.status_code != 200:
            migration_done.set()
            exceptions.put(MigrationFailureException(
                "Failed to migrate partition for the second time."
            ))
            logger.debug(f"Failed to migrate partition for the second time: {from_to_path}")
        else:
            migrated_sizes_queue.put(from_to_path[2])

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        executor.map(sync_partition, migration_list)

    failed_count = failed_items.qsize()
    if failed_count == 0 or not exceptions.empty():
        return

    retry_failed_items = []
    while not failed_items.empty():
        retry_failed_items.append(failed_items.get())

    # Recreate remotes to avoid consistency issues with the rclone remotes
    # It keeps the same remotes names but creates new connections
    recreate_remotes(remotes)

    migration_done.clear()
    # Reduce the number of workers to avoid overloading the rclone API
    # In general, failed items are bigger than successful ones
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(sync_partition_retry, retry_failed_items)


def get_migration_list(src_remote: RCloneRemote,
                       trg_remote: RCloneRemote,
                       partition_paths: list,
                       target_project_id: str,
                       target_table_id: str
                       ):
    migration_list = []
    for source_partition_path, partition_size in partition_paths:
        path_from = (
            f"{src_remote.name}:"
            f"{src_remote.bucket_name}{src_remote.bucket_path}"
            f"{source_partition_path}"
        )

        split_path = source_partition_path.split('/')
        split_path[2] = target_project_id
        split_path[3] = target_table_id
        target_partition_path = "/".join(split_path)

        path_to = (
            f"{trg_remote.name}:"
            f"{trg_remote.bucket_name}{trg_remote.bucket_path}"
            f"{target_partition_path}"
        )

        migration_list.append((path_from, path_to, partition_size))
    return migration_list


def migrate_partition_and_monitor(migration_list: list,
                                  exceptions: Queue,
                                  rc_config: RcloneAPIConfig,
                                  concurrency: int,
                                  remotes: dict,
                                  partitions_size: int
                                  ) -> None:
    migrated_sizes_queue = Queue()
    threading.Thread(
        target=migrate_partitions_threaded,
        args=(migration_list, migrated_sizes_queue, exceptions, rc_config, concurrency, remotes)
    ).start()
    monitor_progress(partitions_size, migrated_sizes_queue, exceptions)


def upload_catalog_and_monitor(profile: ProfileUserContext,
                               catalog: Catalog,
                               reuse_partitions: bool,
                               target_data: MigrationData=None,
                               target_storage_id: str=None,
                               ) -> None:
    total_partitions_count = catalog.get_total_partitions()
    uploaded_count = Queue()
    exceptions = Queue()
    threading.Thread(
        target=update_catalog_and_upload,
        args=(
            profile,
            catalog,
            uploaded_count,
            exceptions,
            target_data,
            target_storage_id,
            reuse_partitions
        )
    ).start()
    monitor_progress(
        total_partitions_count,
        uploaded_count, exceptions,
        unit="units",
        unit_scale=False,
        unit_divisor=1,
        desc="Catalog"
    )


def migrate_data(target_profile: ProfileUserContext,
                 target_data: MigrationData,
                 source_storages: list[dict],
                 catalog: Catalog,
                 rc_config: RcloneAPIConfig,
                 concurrency: int,
                 reuse_partitions: bool = False
                 ) -> None:
    logger.info(f'{" Data Migration ":=^50}')

    if reuse_partitions:
        summary_and_confirm(catalog, reuse_partitions, {})
        upload_catalog_and_monitor(target_profile, catalog, reuse_partitions)
        logger.info('')
        return

    target_storage_id = get_storage_default_by_table(
        target_profile,
        target_data.storages
    )
    partitions_by_storage = catalog.get_partitions_by_storage()
    partitions_size = catalog.get_total_size()

    migration_list = []
    exceptions = Queue()
    remotes = {}

    for source_storage_id, partitions_to_migrate in partitions_by_storage.items():
        try:
            source_remote = get_remote(
                remotes,
                source_storages,
                source_storage_id,
                rc_config,
                "source"
            )
            target_remote = get_remote(
                remotes,
                target_data.storages,
                target_storage_id,
                rc_config,
                "target"
            )
        except Exception as exc:
            exceptions.put(exc)
            close_remotes(remotes)
            raise

        migration_list.extend(
            get_migration_list(
                source_remote,
                target_remote,
                partitions_to_migrate,
                target_data.get_project_id(),
                target_data.get_table_id()
            )
        )

    summary_and_confirm(catalog, reuse_partitions, remotes)
    migrate_partition_and_monitor(
        migration_list,
        exceptions,
        rc_config,
        concurrency,
        remotes,
        partitions_size
    )
    close_remotes(remotes)

    if exceptions.qsize() != 0:
        exception = exceptions.get()
        raise exception

    upload_catalog_and_monitor(
        target_profile,
        catalog,
        reuse_partitions,
        target_data=target_data,
        target_storage_id=target_storage_id
    )
    logger.info('')
