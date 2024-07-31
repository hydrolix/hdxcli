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
    project_table_name = f'{profile.projectname}.{profile.tablename}'
    logger.info(f"{f'Uploading catalog for {project_table_name[:20]}':<42} -> [!n]")
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
