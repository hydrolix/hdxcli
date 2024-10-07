import time
from dataclasses import dataclass, field
from queue import Queue
from typing import Optional, Dict, List

from tqdm import tqdm

from .catalog_operations import Catalog
from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


@dataclass
class MigrationData:
    project: Optional[Dict] = field(default_factory=dict)
    table: Optional[Dict] = field(default_factory=dict)
    functions: List[Dict] = field(default_factory=list)
    dictionaries: List[Dict] = field(default_factory=list)
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


def get_catalog(profile: ProfileUserContext,
                data: MigrationData,
                temp_catalog: bool
                ) -> Catalog:
    project_table_name = f'{profile.projectname}.{profile.tablename}'
    logger.info(        f"{f'Downloading catalog of {project_table_name[:19]}':<42} -> [!n]")
    catalog = Catalog()
    catalog.download(
        profile,
        data.get_project_id(),
        data.get_table_id(),
        temp_catalog=temp_catalog
    )
    logger.info('Done')
    return catalog


def upload_catalog(profile: ProfileUserContext, catalog: Catalog) -> None:
    logger.info(f"{f'Uploading catalog':<42} -> [!n]")
    catalog.upload(profile)
    logger.info('Done')


def update_catalog_and_upload(profile: ProfileUserContext,
                              catalog: Catalog,
                              target_data: MigrationData,
                              target_storage_id: str
                              ) -> None:
    logger.info(f"{f'Updating catalog':<42} -> [!n]")
    project_id = target_data.project.get('uuid')
    table_id = target_data.table.get('uuid')
    catalog.update(project_id, table_id, target_storage_id)
    logger.info('Done')
    upload_catalog(profile, catalog)


def bytes_to_human_readable(amount: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if amount < 1024:
            return f"{amount:.2f} {unit}"
        amount /= 1024
    return f"{amount:.2f} PB"


def confirm_action(prompt: str = 'Continue with migration?') -> bool:
    while True:
        logger.info(f'{prompt} (yes/no): [!i]')
        response = input().strip().lower()
        if response in ['yes', 'no']:
            return response == 'yes'
        logger.info("Invalid input. Please enter 'yes' or 'no'.")


def print_summary(total_rows: int,
                  total_files: int,
                  total_size: int
                  ) -> None:
    logger.info(f'{" Summary ":=^30}')
    logger.info(f'- Total rows: {total_rows}')
    logger.info(f'- Total partitions: {total_files}')
    logger.info(f'- Total size: {bytes_to_human_readable(total_size)}')
    logger.info('')


def monitor_progress(total_bytes, migrated_size_queue, exceptions_queue):
    total_bytes_processed = 0
    progress_bar = tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        bar_format="{desc}{bar:10} {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )

    while total_bytes_processed < total_bytes:
        if not migrated_size_queue.empty():
            bytes_size = migrated_size_queue.get()
            progress_bar.update(bytes_size)
            total_bytes_processed += bytes_size
        else:
            time.sleep(0.5)
        if not exceptions_queue.empty():
            progress_bar.set_description(desc="ERROR")
            progress_bar.close()
            return
    progress_bar.close()
