import csv
import io
import json
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from pathlib import Path
from queue import Queue
from typing import Optional

from hdx_cli.library_api.common import rest_operations as rest_ops
from hdx_cli.library_api.common.exceptions import (
    CatalogException,
    HdxCliException,
    HttpException,
    ResourceNotFoundException,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import ProfileUserContext

logger = get_logger()

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
BASE_URL = "{scheme}://{hostname}/config/v1/orgs/{org_id}/catalog/"


def _get_metadata(metadata):
    return json.dumps(metadata)


def _set_metadata(metadata):
    return json.loads(metadata.replace("'", '"'))


def save_catalog_to_temporal_file(catalog: bytes, project_id: str, table_id: str) -> None:
    temp_dir = Path(tempfile.gettempdir())
    file_path = temp_dir / f"{project_id}_{table_id}_catalog.csv"
    file_path.write_bytes(catalog)


def get_catalog_from_temporal_file(project_id: str, table_id: str) -> list["Partition"]:
    file_path = Path(tempfile.gettempdir()) / f"{project_id}_{table_id}_catalog.csv"
    if not file_path.exists():
        return []

    return _get_catalog_from_bytes(file_path.read_bytes())


def _get_bytes_from_catalog(partitions: list["Partition"]) -> bytes:
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    for partition in partitions:
        csv_writer.writerow(partition.to_list())
    return csv_buffer.getvalue().encode("utf-8")


def _get_catalog_from_bytes(file: bytes) -> list["Partition"]:
    csv_catalog = io.StringIO(file.decode("utf-8"))
    reader = csv.reader(csv_catalog, delimiter=",")
    next(reader)  # Jump csv header
    return [Partition.from_list(row) for row in reader]


def chunked_iterable(iterable: list, chunk_size: int):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


@dataclass
class Partition:
    created: str
    modified: str
    min_timestamp: str
    max_timestamp: str
    manifest_size: int
    data_size: int
    index_size: int
    root_path: str
    data_path: str
    active: str
    rows: int
    mem_size: int
    metadata: dict = field(default_factory=dict)
    shard_key: str = ""
    lock: Optional[str] = None
    storage_id: str = ""

    @staticmethod
    def from_list(values: list[str]) -> "Partition":
        return Partition(
            created=values[0],
            modified=values[1],
            min_timestamp=values[2],
            max_timestamp=values[3],
            manifest_size=int(values[4]),
            data_size=int(values[5]),
            index_size=int(values[6]),
            root_path=values[7],
            data_path=values[8],
            active=values[9],
            rows=int(values[10]),
            mem_size=int(values[11]),
            metadata=_set_metadata(values[12]),
            shard_key=values[13],
            lock=values[14],
            storage_id=values[15],
        )

    def to_list(self) -> list[str]:
        return [
            self.created,
            self.modified,
            self.min_timestamp,
            self.max_timestamp,
            str(self.manifest_size),
            str(self.data_size),
            str(self.index_size),
            self.root_path,
            self.data_path,
            self.active,
            str(self.rows),
            str(self.mem_size),
            _get_metadata(self.metadata),
            self.shard_key,
            self.lock,
            self.storage_id,
        ]

    @property
    def partition_path(self) -> str:
        return f"{self.root_path.strip()}/{self.data_path.strip()}"

    @property
    def partition_size(self) -> int:
        return self.manifest_size + self.index_size + self.data_size


class Catalog:
    def __init__(self):
        self.partitions: list[Partition] = []

    def download(
        self,
        profile: ProfileUserContext,
        project_id: str,
        table_id: str,
        temp_catalog: bool = False,
    ) -> None:
        self.partitions = (
            get_catalog_from_temporal_file(project_id, table_id) if temp_catalog else []
        )
        if self.partitions:
            return

        download_catalog_url = (
            f"{BASE_URL.format(scheme=profile.scheme, hostname=profile.hostname, org_id=profile.org_id)}"
            f"download/?project={project_id}&table={table_id}"
        )
        headers = {
            "Authorization": f"{profile.auth.token_type} {profile.auth.token}",
            "Accept": "application/json",
        }
        try:
            catalog = rest_ops.get(download_catalog_url, headers=headers, fmt="csv", timeout=180)
            self.partitions = _get_catalog_from_bytes(catalog)
            save_catalog_to_temporal_file(catalog, project_id, table_id)
        except HttpException as exc:
            raise HdxCliException(
                f"Some error occurred while downloading the catalog: {exc}"
            ) from exc

    def upload(
        self, profile: ProfileUserContext, uploaded_count: Queue, chunk_size: int = 250
    ) -> None:
        upload_catalog_url = (
            f"{BASE_URL.format(scheme=profile.scheme, hostname=profile.hostname, org_id=profile.org_id)}"
            f"upload/?header=no"
        )
        headers = {
            "Authorization": f"{profile.auth.token_type} {profile.auth.token}",
            "Accept": "application/json",
        }

        self.partitions = sorted(
            self.partitions,
            key=lambda item: datetime.strptime(item.max_timestamp, TIMESTAMP_FORMAT),
        )

        for chunk in chunked_iterable(self.partitions, chunk_size):
            catalog_file = _get_bytes_from_catalog(chunk)
            retries = 3
            for attempt in range(retries):
                try:
                    rest_ops.post_with_file(
                        upload_catalog_url, headers=headers, timeout=60, file_content=catalog_file
                    )
                    uploaded_count.put(len(chunk))
                    time.sleep(1)
                    break
                except HttpException as exc:
                    message_error = str(exc.message)
                    if "existing entries in Catalog" in message_error:
                        uploaded_count.put(len(chunk))
                        logger.debug("Catalog entries already exist, continuing.")
                        time.sleep(1)
                        break

                    if attempt < retries - 1:
                        sleep_time = 2**attempt
                        logger.debug(
                            f"Error uploading catalog, retrying in {sleep_time} seconds: {exc}"
                        )
                        time.sleep(sleep_time)
                    else:
                        message_error = f"An error occurred while uploading the catalog: {exc}."
                        logger.debug(message_error)
                        raise HdxCliException(message_error) from exc

    def update(self, project_uuid: str, table_uuid: str, target_storage_uuid: str) -> None:
        for partition in self.partitions:
            partition.root_path = f"{project_uuid}/{table_uuid}"
            partition.metadata["storage_id"] = target_storage_uuid
            partition.storage_id = target_storage_uuid
            # This mitigates problems when there was some deleted alter job, without cancellation.
            partition.lock = None

    def update_with_shared_storages(self, equivalent_storages: dict[str, str]) -> None:
        for partition in self.partitions:
            new_storage_uuid = equivalent_storages.get(partition.storage_id)

            if not new_storage_uuid and not partition.storage_id:
                new_storage_uuid = equivalent_storages.get("default")

            if not new_storage_uuid:
                raise ResourceNotFoundException(
                    f"The storage with uuid '{partition.storage_id}' was not found "
                    "in the destination cluster."
                )
            partition.storage_id = new_storage_uuid
            partition.metadata["storage_id"] = new_storage_uuid
            # This mitigates problems when there was some deleted alter job, without cancellation.
            partition.lock = None

    def filter_by_timestamp(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> None:
        if not (from_date or to_date):
            return

        self.partitions = list(
            filter(
                lambda item: (
                    not from_date
                    or datetime.strptime(item.min_timestamp, TIMESTAMP_FORMAT) >= from_date
                )
                and (
                    not to_date
                    or datetime.strptime(item.max_timestamp, TIMESTAMP_FORMAT) <= to_date
                ),
                self.partitions,
            )
        )
        if not self.partitions:
            raise CatalogException("No partitions found matching the given date range.")

    def get_summary(self) -> tuple[int, int, int]:
        row_count = sum(int(p.rows) for p in self.partitions)
        return row_count, self.partitions_count, self.total_size

    @property
    def partitions_count(self) -> int:
        return len(self.partitions)

    @cached_property
    def total_size(self) -> int:
        return sum(p.partition_size for p in self.partitions)

    def get_partitions_by_storage(self) -> dict[str, list[tuple[str, int]]]:
        partitions_by_storage = {}
        for partition in self.partitions:
            split_path = partition.partition_path.split("/")
            # Add 'db/hdx' to the partition path
            split_path.insert(0, "db/hdx")
            partition_path = "/".join(split_path)
            partition_size = partition.manifest_size + partition.index_size + partition.data_size
            partition_files_path = [(partition_path, partition_size)]

            storage_id = partition.storage_id
            if partitions_by_storage.get(storage_id):
                partitions_by_storage[storage_id].extend(partition_files_path)
            else:
                partitions_by_storage[storage_id] = partition_files_path
        return partitions_by_storage
