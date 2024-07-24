import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from hdx_cli.cli_interface.migrate.helpers import (print_summary, validate_files_amount,
                                                   confirm_migration, MigrationData,
                                                   show_progress_bar, set_catalog)
from hdx_cli.cli_interface.migrate.recovery import recovery_process
from hdx_cli.cli_interface.migrate.workers import CountingQueue, ReaderWorker, WriterWorker
from hdx_cli.library_api.common.catalog import Catalog
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.provider import get_provider
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


def show_and_confirm_data_migration(catalog: Catalog, migrated_file_list: list) -> bool:
    # General files information
    total_rows, total_files, total_size = catalog.get_summary_information()
    # Migrated files
    migrated_files_count = len(migrated_file_list)
    # Show it
    print_summary(total_rows, total_files, total_size, migrated_files_count)
    return validate_files_amount(total_files, migrated_files_count) and confirm_migration()


def migrate_data(target_profile: ProfileUserContext, target_data: MigrationData,
                 source_storages: list[dict], catalog: Catalog, workers_amount: int,
                 recovery: bool = False, reuse_partitions: bool = False) -> None:
    logger.info(f'{" Data ":=^50}')

    if not reuse_partitions:
        partition_paths_by_storage = catalog.get_partition_files_by_storage()
        partitions_size = catalog.get_total_size()

        # Migrating partitions
        migrated_files_queue = Queue()
        writer_queue = Queue()
        exceptions = Queue()

        workers_list = []
        providers = {}
        files_count = 0

        migrated_file_list = []
        # Recovery point if exist
        if recovery:
            target_root_path = f'db/hdx/{target_data.get_project_id()}/{target_data.get_table_id()}'
            target_provider = get_provider(providers, target_data.storages)

            logger.info(f"{'Looking migrated files':<42} -> [!n]")
            migrated_file_list = target_provider.list_files_in_path(path=target_root_path)
            logger.info('Done')

        for storage_id, files_to_migrate in partition_paths_by_storage.items():
            # Creating data providers
            try:
                source_provider = get_provider(providers, source_storages, storage_id)
                target_provider = get_provider(providers, target_data.storages)
            except Exception as exc:
                exceptions.put(exc)
                raise

            # Total amount of files to migrate
            files_count += len(files_to_migrate)

            # Are there files already migrated?
            if migrated_file_list:
                files_to_migrate = recovery_process(files_to_migrate, migrated_file_list,
                                                    migrated_files_queue)

            reader_queue = CountingQueue(files_to_migrate)
            for _ in range(workers_amount):
                workers_list.append(
                    ReaderWorker(reader_queue, writer_queue, exceptions,  source_provider,
                                 workers_amount,  target_data.get_project_id(),
                                 target_data.get_table_id())
                )
                workers_list.append(
                    WriterWorker(
                        writer_queue, migrated_files_queue, exceptions, target_provider)
                )

        # Before start the migration, show and ask for confirmation
        if not show_and_confirm_data_migration(catalog, migrated_file_list):
            logger.info(f'{" Migration Process Finished ":=^50}')
            logger.info('')
            sys.exit(0)

        # Start all workers once confirmed by the user
        with ThreadPoolExecutor(max_workers=workers_amount * 2) as executor:
            future_to_worker = {executor.submit(worker.start): worker for worker in workers_list}

            # Show progress bar while workers are running
            show_progress_bar(partitions_size, migrated_files_queue, exceptions)

            # Signal writers to stop by putting 'None' in the queue
            for _ in range(workers_amount * len(partition_paths_by_storage)):
                writer_queue.put(None)

            # Wait for all workers to complete
            for future in as_completed(future_to_worker):
                try:
                    future.result()
                except Exception as exc:
                    exceptions.put(exc)

        if exceptions.qsize() != 0:
            exception = exceptions.get()
            raise exception

    set_catalog(target_profile, target_data, catalog, reuse_partitions)
    logger.info('')
