from google.cloud.storage import Client
from google.cloud.exceptions import NotFound, Forbidden, GoogleCloudError
from google.auth.exceptions import DefaultCredentialsError
from google.resumable_media.common import DataCorruption

from .base_provider import BaseProvider
from ..common.exceptions import CloudConnectionError, CredentialsNotFoundError
from ..common.logging import get_logger

logger = get_logger()

CONTENT_TYPE = 'application/octet-stream'
# Use a large timeout to write solves error like ->
# ('Connection aborted.', TimeoutError('The write operation timed out'))
LARGE_TIMEOUT = 1800


def _retry_read_file(bucket, path):
    """Retry the download for a blob."""
    blob = bucket.blob(path)
    return blob.download_as_bytes()


class GcpProvider(BaseProvider):
    def __init__(self, bucket_name, bucket_path, region):
        self.client = None
        self.bucket = None
        self.bucket_name = bucket_name
        self.region = region
        partial_bucket_path = bucket_path[1:] if bucket_path.startswith('/') else bucket_path
        self.bucket_path = partial_bucket_path + '/' if partial_bucket_path else partial_bucket_path

    def setup_connection_to_bucket(self, **kwargs):
        logger.info(f'Connecting to GCP bucket: {self.bucket_name}')

        google_application_credentials = kwargs.get('google_application_credentials')
        while not google_application_credentials:
            logger.info('  Enter the Google credentials JSON file path: [!i]')
            google_application_credentials = input().strip()

        try:
            self.client = Client.from_service_account_json(google_application_credentials)
            self.bucket = self.client.get_bucket(self.bucket_name)
            return self
        except FileNotFoundError as exc:
            raise CredentialsNotFoundError() from exc
        except DefaultCredentialsError as exc:
            raise CredentialsNotFoundError() from exc

    def read_file(self, path: str) -> bytes:
        try:
            blob = self.bucket.blob(f'{self.bucket_path}{path}')
            return blob.download_as_bytes()
        except NotFound as exc:
            raise FileNotFoundError(f"File '{path}' not found in '{self.bucket_name}' bucket.") from exc
        except Forbidden as exc:
            raise PermissionError(f"Access denied to read file '{path}' in '{self.bucket_name}' bucket.") from exc
        except GoogleCloudError as exc:
            raise CloudConnectionError(f"An error occurred while reading file '{path}': {exc}") from exc
        except DataCorruption:
            return _retry_read_file(self.bucket, f'{self.bucket_path}{path}')

    def write_file(self, path: str, data: bytes):
        try:
            blob = self.bucket.blob(f'{self.bucket_path}{path}')
            blob.upload_from_string(data, content_type=CONTENT_TYPE, timeout=LARGE_TIMEOUT)
        except NotFound as exc:
            raise FileNotFoundError(f"Destination '{path}' not found in bucket.") from exc
        except Forbidden as exc:
            raise PermissionError(f"Access denied to write file '{path}' in '{self.bucket_name}' bucket.") from exc
        except GoogleCloudError as exc:
            raise CloudConnectionError(f"An error occurred while writing file '{path}': {exc}") from exc

    def list_files_in_path(self, path: str) -> list:
        blobs = self.bucket.list_blobs(prefix=path)
        file_paths = []
        for blob in blobs:
            # Only add file names, not directories
            if not blob.name.endswith('/'):
                _, _, file_path = blob.name.partition('/data/v2/current/')
                file_paths.append(file_path)

        return file_paths
