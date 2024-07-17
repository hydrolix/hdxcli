from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError, HttpResponseError, ClientAuthenticationError

from .base_provider import BaseProvider
from ..common.exceptions import CloudConnectionError, CredentialsNotFoundError
from ..common.logging import get_logger

logger = get_logger()


class AzureProvider(BaseProvider):
    def __init__(self, bucket_name, bucket_path, region):
        self.blob_service_client = None
        self.bucket_name = bucket_name
        self.region = region
        partial_bucket_path = bucket_path[1:] if bucket_path.startswith('/') else bucket_path
        self.bucket_path = partial_bucket_path + '/' if partial_bucket_path else partial_bucket_path

    def setup_connection_to_bucket(self, **kwargs):
        logger.info(f'Connecting to Azure bucket: {self.bucket_name}')

        connection_string = kwargs.get('connection_string')
        while not connection_string:
            logger.info('  Please enter your connection string: [!i]')
            connection_string = input().strip()

        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = self.blob_service_client.get_container_client(self.bucket_name)
            if not container_client.exists():  # pylint: disable=E1120
                raise FileNotFoundError(f"Container '{self.bucket_name}' not found in Azure storage account.")
            return self
        except ClientAuthenticationError as exc:
            raise CredentialsNotFoundError('There was credential error when trying to connect '
                                           'to Azure storage account') from exc

    def read_file(self, path: str) -> bytes:
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.bucket_name,
                                                                   blob=f'{self.bucket_path}{path}')
            return blob_client.download_blob().readall()
        except ResourceNotFoundError as exc:
            raise FileNotFoundError(exc) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise PermissionError(
                    f"Access denied to read file '{self.bucket_path}{path}' in container '{self.bucket_name}'.") from exc
            else:
                raise CloudConnectionError(
                    f"An error occurred while reading file '{self.bucket_path}{path}': {exc}") from exc
        except AzureError as exc:
            raise ConnectionError(f"An error occurred while reading file '{self.bucket_path}{path}': {exc}") from exc

    def write_file(self, path: str, data: bytes):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.bucket_name,
                                                                   blob=f'{self.bucket_path}{path}')
            blob_client.upload_blob(data, overwrite=True)
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise PermissionError(
                    f"Access denied to write file '{path}' in container '{self.bucket_name}'.") from exc
            else:
                raise CloudConnectionError(f"An error occurred while writing file '{path}': {exc}") from exc
        except AzureError as exc:
            raise CloudConnectionError(f"An error occurred while writing file '{path}': {exc}") from exc

    def list_files_in_path(self, path: str) -> list:
        container_client = self.blob_service_client.get_container_client(self.bucket_name)
        blob_list = container_client.list_blobs(name_starts_with=path)
        file_paths = []
        for blob in blob_list:
            # Only add file names, not directories
            if not blob.name.endswith('/'):
                _, _, file_path = blob.name.partition('/data/v2/current/')
                file_paths.append(file_path)

        return file_paths
