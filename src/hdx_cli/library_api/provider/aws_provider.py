import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from .base_provider import BaseProvider
from ..common.exceptions import CloudConnectionError, CredentialsNotFoundError
from ..common.logging import get_logger

logger = get_logger()


class AwsProvider(BaseProvider):
    def __init__(self, bucket_name, bucket_path, region):
        self.s3_client = None
        self.bucket_name = bucket_name
        self.region = region
        partial_bucket_path = bucket_path[1:] if bucket_path.startswith('/') else bucket_path
        self.bucket_path = partial_bucket_path + '/' if partial_bucket_path else partial_bucket_path

    def setup_connection_to_bucket(self, **kwargs):
        logger.info(f'Connecting to AWS bucket: {self.bucket_name}')

        aws_access_key_id = kwargs.get('aws_access_key_id')
        aws_secret_access_key = kwargs.get('aws_secret_access_key')
        while not aws_access_key_id or not aws_secret_access_key:
            logger.info('  Please enter your access key ID: [!i]')
            aws_access_key_id = input().strip()
            logger.info('  Please enter your secret access ID: [!i]')
            aws_secret_access_key = input().strip()

        try:
            self.s3_client = boto3.client('s3',
                                          aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key)
            self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            return self
        except NoCredentialsError as exc:
            raise CredentialsNotFoundError('AWS credentials not found. Please provide valid credentials.') from exc
        except ClientError as exc:
            error_code = exc.response['Error']['Code']
            if error_code == 'AccessDenied':
                raise PermissionError(
                    f"Access denied to bucket '{self.bucket_name}'. Please check your permissions.") from exc
            else:
                raise CloudConnectionError(
                    f"There was an error setting up connection to bucket '{self.bucket_name}': {exc}") from exc

    def read_file(self, path: str) -> bytes:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=f'{self.bucket_path}{path}')
            return response['Body'].read()
        except ClientError as exc:
            error_code = exc.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise FileNotFoundError(f"File '{path}' not found in '{self.bucket_name}' bucket.") from exc
            elif error_code == 'AccessDenied':
                raise PermissionError(f"Access denied to read file '{path}' in '{self.bucket_name}' bucket.") from exc
            else:
                raise CloudConnectionError(f"There was an error reading file '{path}': {exc}") from exc
        except NoCredentialsError as exc:
            raise CredentialsNotFoundError() from exc

    def write_file(self, path: str, data: bytes):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=f'{self.bucket_path}{path}', Body=data)
        except ClientError as exc:
            raise CloudConnectionError(f"There was an error writing file '{path}': {exc}") from exc
        except NoCredentialsError as exc:
            raise CredentialsNotFoundError() from exc

    def list_files_in_path(self, path: str) -> list:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=path)
        file_paths = []
        if 'Contents' in response:
            for obj in response['Contents']:
                # Only add file names, not directories
                if not obj['Key'].endswith('/'):
                    _, _, file_path = obj['Key'].partition('/data/v2/current/')
                    file_paths.append(file_path)

        return file_paths
