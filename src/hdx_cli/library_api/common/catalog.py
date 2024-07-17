import csv
import io
import json
from datetime import datetime
from functools import reduce

from hdx_cli.library_api.common import rest_operations as rest_ops
from hdx_cli.library_api.common.exceptions import (ResourceNotFoundException, HttpException,
                                                   HdxCliException, CatalogException)

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


def _get_metadata(metadata):
    return json.dumps(metadata)


def _set_metadata(metadata):
    return json.loads(metadata.replace("'", '"'))


class Partition:
    def __init__(self, values):
        self.created = values[0]
        self.modified = values[1]
        self.min_timestamp = values[2]
        self.max_timestamp = values[3]
        self.manifest_size = values[4]
        self.data_size = values[5]
        self.index_size = values[6]
        self.root_path = values[7]
        self.data_path = values[8]
        self.active = values[9]
        self.rows = values[10]
        self.mem_size = values[11]
        self.metadata = _set_metadata(values[12])
        self.shard_key = values[13]
        self.lock = values[14]
        self.storage_id = values[15]

    def get_partition_path(self):
        return "/".join([str(self.root_path).strip(), str(self.data_path).strip()])

    def to_list(self):
        return [self.created, self.modified, self.min_timestamp, self.max_timestamp,
                self.manifest_size, self.data_size, self.index_size, self.root_path,
                self.data_path, self.active, self.rows, self.mem_size,
                _get_metadata(self.metadata), self.shard_key, self.lock, self.storage_id]

    def get_manifest_size(self):
        return int(self.manifest_size)

    def get_index_size(self):
        return int(self.index_size)

    def get_data_size(self):
        return int(self.data_size)

    def get_partition_size(self):
        return self.get_manifest_size() + self.get_index_size() + self.get_data_size()


def _get_bytes_from_catalog(partitions: list[Partition]) -> bytes:
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    for partition in partitions:
        csv_writer.writerow(partition.to_list())

    return csv_buffer.getvalue().encode('utf-8')


def _get_catalog_from_bytes(file: bytes) -> list[Partition]:
    csv_catalog = io.StringIO(file.decode('utf-8'))
    reader = csv.reader(csv_catalog, delimiter=',')
    # Jump csv header
    reader.__next__()
    return [Partition(row) for row in reader]


class Catalog:
    def __init__(self):
        self.partitions = []
        self.total_size = 0

    def download(self, profile, project_id, table_id) -> None:
        download_catalog_url = (
            f'{profile.scheme}://{profile.hostname}/config/v1/orgs/{profile.org_id}/'
            f'catalog/download/?project={project_id}&table={table_id}')
        headers = {'Authorization': f"{profile.auth.token_type} {profile.auth.token}",
                   'Accept': 'application/json'}
        try:
            catalog = rest_ops.get(download_catalog_url, headers=headers, fmt='csv', timeout=180)
            self.partitions = _get_catalog_from_bytes(catalog)
        except HttpException as exc:
            raise HdxCliException(f"The Hydrolix version in the '{profile.hostname}' hostname "
                                  f"does not support catalog endpoint.") from exc

    def upload(self, profile) -> None:
        upload_catalog_url = (
            f'{profile.scheme}://{profile.hostname}/config/v1/orgs/{profile.org_id}/'
            f'catalog/upload/?header=no')
        headers = {'Authorization': f"{profile.auth.token_type} {profile.auth.token}",
                   'Accept': 'application/json'}

        catalog_file = _get_bytes_from_catalog(self.partitions)
        rest_ops.create_file(upload_catalog_url, headers=headers, file_stream=catalog_file,
                             timeout=300, remote_filename=None)

    def update(self, project_uuid: str, table_uuid: str, storage_uuid: str) -> None:
        for partition in self.partitions:
            partition.root_path = f'{project_uuid}/{table_uuid}'
            partition.metadata['storage_id'] = f'{storage_uuid}'
            partition.storage_id = f'{storage_uuid}'
            # This mitigates problems when there was some deleted alter job, without cancellation.
            partition.lock = None

    def update_equivalent_storage(self, equivalent_storages: dict[str, str]) -> None:
        for partition in self.partitions:
            new_storage_uuid = equivalent_storages.get(partition.storage_id)

            if not new_storage_uuid and not partition.storage_id:
                new_storage_uuid = equivalent_storages.get('default')

            if not new_storage_uuid:
                raise ResourceNotFoundException(
                    f"The storage with uuid '{partition.storage_id}' was not found "
                    "in the destination cluster.")

            partition.storage_id = new_storage_uuid
            partition.metadata['storage_id'] = f'{new_storage_uuid}'
            # This mitigates problems when there was some deleted alter job, without cancellation.
            partition.lock = None

    def filter_by_timestamp(self, min_timestamp: datetime, max_timestamp: datetime) -> None:
        if not (min_timestamp or max_timestamp):
            return

        self.partitions = list(filter(
            lambda item: (not min_timestamp or datetime.strptime(item.min_timestamp,
                                                                 TIMESTAMP_FORMAT) >= min_timestamp) and
                         (not max_timestamp or datetime.strptime(item.max_timestamp,
                                                                 TIMESTAMP_FORMAT) <= max_timestamp),
            self.partitions
        ))

        if not self.partitions:
            raise CatalogException("No partitions found matching the given date range.")

    def get_summary_information(self) -> tuple[int, int, int]:
        row_count = reduce(lambda count, item: count + int(item.rows), self.partitions, 0)
        return row_count, len(self.partitions) * 3, self.get_total_size()

    def get_total_size(self) -> int:
        if not self.total_size:
            self.total_size = reduce(lambda count, item: count + item.get_partition_size(),
                                     self.partitions, 0)

        return self.total_size

    def get_partition_files_by_storage(self) -> dict[str, list[tuple[str, int]]]:
        partition_files_by_storage = {}
        for partition in self.partitions:
            split_path = partition.get_partition_path().split('/')
            # Add 'db/hdx' to the partition path
            split_path.insert(0, 'db/hdx')
            partition_path = '/'.join(split_path)
            partition_files_path = [(f'{partition_path}/manifest.hdx', partition.get_manifest_size()),
                                    (f'{partition_path}/index.hdx', partition.get_index_size()),
                                    (f'{partition_path}/data.hdx', partition.get_data_size())]

            storage_id = partition.storage_id
            if partition_files_by_storage.get(storage_id):
                partition_files_by_storage[storage_id].extend(partition_files_path)
            else:
                partition_files_by_storage[storage_id] = partition_files_path

        return partition_files_by_storage
