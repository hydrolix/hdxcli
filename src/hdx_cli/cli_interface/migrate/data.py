import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from .helpers import (
    print_summary,
    confirm_action,
    MigrationData,
    update_catalog_and_upload, upload_catalog, monitor_progress
)
from .rc.rc_remotes import RCloneRemote
from .rc.rc_utils import get_remote, close_remotes
from .catalog_operations import Catalog
from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_storage_default_by_table

from hdx_cli.library_api.common.rest_operations import post_with_retries
from ...library_api.common.exceptions import HdxCliException

logger = get_logger()


def show_and_confirm_data_migration(catalog: Catalog) -> bool:
    """
    Displays a summary of the migration process,
    validates that the number of files to migrate is greater than 0,
    and asks for user confirmation to start the migration process.
    """
    total_rows, total_partitions, total_size = catalog.get_summary_information()
    print_summary(total_rows, total_partitions, total_size)
    return confirm_action()


def migrate_partitions_threaded(migration_list: list,
                                migrated_size_queue: Queue,
                                exceptions: Queue,
                                rc_config: RcloneAPIConfig,
                                concurrency: int
                                ):
    base_url = rc_config.get_url()
    url = f"{base_url}/sync/copy"

    def sync_partition(from_to_path):
        data = {"srcFs": from_to_path[0], "dstFs": from_to_path[1]}
        response = post_with_retries(url, data, user=rc_config.user, password=rc_config.password)
        if not response or response.status_code != 200:
            exception = HdxCliException(
                f"Failed to migrate partition: {response.json() if response else 'No response.'}"
            )
            exceptions.put(exception)
            raise exception
        else:
            migrated_size_queue.put(from_to_path[2])

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        executor.map(sync_partition, migration_list)


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


def migrate_data(target_profile: ProfileUserContext,
                target_data: MigrationData,
                source_storages: list[dict],
                catalog: Catalog,
                rc_config: RcloneAPIConfig,
                concurrency: int,
                reuse_partitions: bool = False
                ) -> None:
    logger.info(f'{" Data ":=^50}')

    if reuse_partitions:
        upload_catalog(target_profile, catalog)
        logger.info('')
        return

    target_storage_id = get_storage_default_by_table(
        target_profile,
        target_data.storages
    )
    partitions_by_storage = catalog.get_partitions_by_storage()
    partitions_size = catalog.get_total_size()

    migration_list = []
    migrated_size_queue = Queue()
    exceptions = Queue()
    remotes = {}

    for source_storage_id, partitions_to_migrate in partitions_by_storage.items():
        try:
            source_remote = get_remote(remotes, source_storages, source_storage_id, rc_config)
            target_remote = get_remote(remotes, target_data.storages, target_storage_id, rc_config)
        except Exception as exc:
            exceptions.put(exc)
            close_remotes(remotes, rc_config)
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

    if not show_and_confirm_data_migration(catalog):
        logger.info(f'{" Migration Process Finished ":=^50}')
        logger.info('')
        sys.exit(0)

    threading.Thread(
        target=migrate_partitions_threaded,
        args=(migration_list, migrated_size_queue, exceptions, rc_config, concurrency)
    ).start()

    monitor_progress(partitions_size, migrated_size_queue, exceptions)

    close_remotes(remotes, rc_config)
    if exceptions.qsize() != 0:
        exception = exceptions.get()
        raise exception

    update_catalog_and_upload(target_profile, catalog, target_data, target_storage_id)
    logger.info('')
