import sys
import threading
import time
from dataclasses import dataclass, field
from queue import Queue
from typing import Dict, List, Optional

from tqdm import tqdm

from hdx_cli.library_api.common.logging import get_logger

from ...library_api.common.exceptions import HdxCliException
from ...models import ProfileUserContext
from .catalog_operations import Catalog

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
        return self.project.get("uuid")

    def get_table_id(self) -> Optional[str]:
        if self.table is None:
            return None
        return self.table.get("uuid")


def get_catalog(profile: ProfileUserContext, data: MigrationData, temp_catalog: bool) -> Catalog:
    project_table_name = f"{profile.projectname}.{profile.tablename}"
    logger.info(f"{f'  Catalog: {project_table_name[:31]}':<42} -> [!n]")
    catalog = Catalog()
    catalog.download(profile, data.get_project_id(), data.get_table_id(), temp_catalog=temp_catalog)
    logger.info("Done")
    return catalog


def update_catalog_and_upload(
    profile: ProfileUserContext,
    catalog: Catalog,
    uploaded_count: Queue,
    exceptions: Queue,
    upload_done: threading.Event,
    target_data: MigrationData,
    target_storage_id: str,
    reuse_partitions,
) -> None:
    try:
        if not reuse_partitions:
            project_id = target_data.project.get("uuid")
            table_id = target_data.table.get("uuid")
            catalog.update(project_id, table_id, target_storage_id)

        catalog.upload(profile, uploaded_count)
    except HdxCliException as exc:
        logger.debug(f"Error while uploading catalog: {exc}")
        exceptions.put(exc)

    upload_done.set()


def bytes_to_human_readable(amount: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if amount < 1024:
            return f"{amount:.2f} {unit}"
        amount /= 1024
    return f"{amount:.2f} PB"


def confirm_action(prompt: str = "Confirm this action?") -> bool:
    while True:
        logger.info(f"{prompt} (yes/no): [!i]")
        response = input().strip().lower()
        if response in ["yes", "y"]:
            return True
        if response in ["no", "n"]:
            return False
        logger.info("Invalid input. Please enter 'yes' or 'no'.")


def print_summary(
    source_hostname: str,
    source_table: str,
    target_hostname: str,
    target_table: str,
    rows: int,
    partitions: int,
    size: int,
) -> None:
    logger.info(f"{' MIGRATION SUMMARY ':=^50}")
    logger.info("- Source:")
    logger.info(f"    Hostname: {source_hostname}")
    logger.info(f"    Table: {source_table}")
    logger.info("- Target:")
    logger.info(f"    Hostname: {target_hostname}")
    logger.info(f"    Table: {target_table}")
    logger.info("- Data:")
    logger.info(f"    Rows: {rows}")
    logger.info(f"    Partitions: {partitions}")
    logger.info(f"    Size: {bytes_to_human_readable(size)}")
    logger.info(f"{'=' * 50}")
    logger.info("")


def monitor_progress(
    total_count: int,
    migrated_queue: Queue,
    exceptions_queue: Queue,
    event_done: threading.Event,
    unit: str = "B",
    unit_scale: bool = True,
    unit_divisor: int = 1024,
    desc: str = "Partitions",
):
    total_bytes_processed = 0
    progress_bar = tqdm(
        total=total_count,
        unit=unit,
        unit_scale=unit_scale,
        unit_divisor=unit_divisor,
        desc=desc,
        bar_format="{desc} {bar:10} {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )

    while total_bytes_processed < total_count:
        if not migrated_queue.empty():
            bytes_size = migrated_queue.get()
            progress_bar.update(bytes_size)
            total_bytes_processed += bytes_size
        else:
            time.sleep(0.5)
        if not exceptions_queue.empty():
            progress_bar.set_description(desc="Error, finishing")
            event_done.wait()
            break
    progress_bar.close()


def cancel_migration():
    logger.info("")
    logger.info(f"{' Migration Cancelled ':=^50}")
    sys.exit(0)
