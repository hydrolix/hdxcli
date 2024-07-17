import time
from dataclasses import dataclass, field
from datetime import datetime
from queue import Queue
from typing import Optional, Dict, List

from tqdm import tqdm

from hdx_cli.library_api.common.catalog import Catalog
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.storage import get_storage_default
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


@dataclass
class MigrationData:
    project: Optional[Dict] = field(default_factory=dict)
    table: Optional[Dict] = field(default_factory=dict)
    transforms: List[Dict] = field(default_factory=list)
    storages: List[Dict] = field(default_factory=list)

    def get_project_id(self) -> Optional[str]:
        if self.project is None:
            return None
        return self.project.get('uuid')

    def get_table_id(self) -> Optional[str]:
        if self.table is None:
            return None
        return self.table.get('uuid')


def get_catalog(profile: ProfileUserContext, data: MigrationData) -> Catalog:
    logger.info(
        f"{f'Downloading catalog of {profile.projectname}.{profile.tablename}':<42} -> [!n]")
    catalog = Catalog()
    catalog.download(profile, data.get_project_id(), data.get_table_id())
    logger.info('Done')
    return catalog


def set_catalog(profile: ProfileUserContext, data: MigrationData,
                catalog: Catalog, reuse_partitions: bool) -> None:
    logger.info(f"{f'Uploading catalog for {profile.projectname}.{profile.tablename}':<42} -> [!n]")
    if not reuse_partitions:
        target_default_storage_id, _ = get_storage_default(data.storages)
        catalog.update(data.get_project_id(), data.get_table_id(), target_default_storage_id)
    catalog.upload(profile)
    logger.info('Done')


def filter_catalog(catalog: Catalog, start_date: datetime = False,
                   end_date: datetime = False) -> None:
    if not (start_date or end_date):
        return

    logger.info(f"{'  Filtering catalog by timestamp':<42} -> [!n]")
    catalog.filter_by_timestamp(start_date, end_date)
    logger.info('Done')


def bytes_to_mb(amount: int) -> float:
    return round(amount / (1024 * 1024), 2)


def show_progress_bar(total_bytes: int, migrated_files_queue: Queue, exceptions: Queue) -> None:
    progress_bar = tqdm(total=total_bytes, desc='Copying ',
                        bar_format="{desc}{bar:15} {percentage:3.0f}%| Elapsed time: {elapsed}")
    total_bytes_processed = 0
    total_files_processed = 0
    while total_bytes_processed < total_bytes:
        if migrated_files_queue.qsize() != 0:
            _, bytes_size = migrated_files_queue.get()
            progress_bar.update(bytes_size)
            total_bytes_processed += bytes_size
            total_files_processed += 1
        else:
            time.sleep(1)

        if not exceptions.empty():
            progress_bar.set_description(desc="Error ")
            break
    progress_bar.close()


def confirm_migration(prompt: str = 'Continue with migration?') -> bool:
    while True:
        logger.info(f'{prompt} (yes/no): [!i]')
        response = input().strip().lower()
        if response in ['yes', 'no']:
            return response == 'yes'
        logger.info("Invalid input. Please enter 'yes' or 'no'.")


# def show_and_confirm_migration(catalog: Catalog, migrated_file_list: list) -> bool:
#     # General files information
#     total_rows, total_files, total_size = catalog.get_summary_information()
#     # Migrated files
#     migrated_files_count = len(migrated_file_list)
#     # Show it
#     print_summary(total_rows, total_files, total_size, migrated_files_count)
#     return validate_files_amount(total_files, migrated_files_count) and confirm_migration()


def print_summary(total_rows: int, total_files: int, total_size: int,
                  migrated_files_count: int = None) -> None:
    logger.info('')
    logger.info(f'{" Summary ":=^30}')
    logger.info(f'- Total rows: {total_rows}')
    logger.info(f'- Total files: {total_files}')
    logger.info(f'- Total size: {bytes_to_mb(total_size)} MB')
    logger.info('')
    if migrated_files_count:
        logger.info(f'- Files already migrated: {migrated_files_count}')


def validate_files_amount(total_files: int, migrated_files: int) -> bool:
    result = True
    if total_files - migrated_files <= 0:
        logger.info('No files to migrate.')
        logger.info('')
        result = False
    return result


# def migrate_data(target_profile: ProfileUserContext, target_data: MigrationData,
#                  source_storages: list[dict], catalog: Catalog, workers_amount: int,
#                  recovery: bool = False, reuse_partitions: bool = False) -> None:
#     logger.info(f'{" Data ":=^50}')
#
#     if not reuse_partitions:
#         partition_paths_by_storage = catalog.get_partition_files_by_storage()
#         partitions_size = catalog.get_total_size()
#
#         # Migrating partitions
#         migrated_files_queue = Queue()
#         writer_queue = Queue()
#         exceptions = Queue()
#
#         workers_list = []
#         providers = {}
#         files_count = 0
#
#         migrated_file_list = []
#         # Recovery point if exist
#         if recovery:
#             target_root_path = f'db/hdx/{target_data.get_project_id()}/{target_data.get_table_id()}'
#             target_provider = get_provider(providers, target_data.storages)
#
#             logger.info(f"{'Looking migrated files':<42} -> [!n]")
#             migrated_file_list = target_provider.list_files_in_path(path=target_root_path)
#             logger.info('Done')
#
#         for storage_id, files_to_migrate in partition_paths_by_storage.items():
#             # Creating data providers
#             try:
#                 source_provider = get_provider(providers, source_storages, storage_id)
#                 target_provider = get_provider(providers, target_data.storages)
#             except Exception as exc:
#                 exceptions.put(exc)
#                 raise
#
#             # Total amount of files to migrate
#             files_count += len(files_to_migrate)
#
#             # Are there files already migrated?
#             if migrated_file_list:
#                 files_to_migrate = recovery_process(files_to_migrate, migrated_file_list, migrated_files_queue)
#
#             reader_queue = CountingQueue(files_to_migrate)
#             for _ in range(workers_amount):
#                 workers_list.append(
#                     ReaderWorker(reader_queue, writer_queue, exceptions,  source_provider,
#                                  workers_amount,  target_data.get_project_id(), target_data.get_table_id())
#                 )
#                 workers_list.append(
#                     WriterWorker(
#                         writer_queue, migrated_files_queue, exceptions, target_provider)
#                 )
#
#         # Before start the migration, show and ask for confirmation
#         if not show_and_confirm_migration(catalog, migrated_file_list):
#             logger.info(f'{" Migration Process Finished ":=^50}')
#             logger.info('')
#             sys.exit(0)
#
#         # Start all workers once confirmed by the user
#         with ThreadPoolExecutor(max_workers=workers_amount * 2) as executor:
#             future_to_worker = {executor.submit(worker.start): worker for worker in workers_list}
#
#             # Show progress bar while workers are running
#             show_progress_bar(partitions_size, migrated_files_queue, exceptions)
#
#             # Signal writers to stop by putting `None` in the queue
#             for _ in range(workers_amount * len(partition_paths_by_storage)):
#                 writer_queue.put(None)
#
#             # Wait for all workers to complete
#             for future in as_completed(future_to_worker):
#                 # worker = future_to_worker[future]
#                 try:
#                     future.result()
#                 except Exception as exc:
#                     exceptions.put(exc)
#
#         if exceptions.qsize() != 0:
#             exception = exceptions.get()
#             raise exception
#
#     set_catalog(target_profile, target_data, catalog, reuse_partitions)
#     logger.info('')
