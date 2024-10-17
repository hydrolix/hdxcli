import json
import os
import random
import string

from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.library_api.common.exceptions import RCloneRemoteException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.rest_operations import post_with_retries

logger = get_logger()

def generate_random_string(length=3):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _get_azure_config():
    logger.info("Enter Azure account: [!i]")
    account = input().strip()
    logger.info("Enter Azure key: [!i]")
    key = input().strip()
    if not account or not key:
        raise ValueError("Azure account and key must not be empty.")

    credentials = {
        "account": account,
        "key": key
    }
    config = {"type": "azureblob", "parameters": credentials}
    return config


def _get_gcp_config(remote):
    logger.info("Enter path to Google Service Account JSON file: [!i]")
    json_path = input().strip()
    if not os.path.isfile(json_path):
        raise ValueError("Invalid path for Google Service Account JSON file.")

    with open(json_path, "r") as file:
        google_service_account = json.load(file)
    credentials_string = json.dumps(google_service_account, separators=(",", ":"))

    config = {
        "type": "gcs",
        "parameters": {
            "service_account_credentials": credentials_string,
            "bucket_policy_only": True,
            "location": remote.region,
        },
    }
    return config


def _get_aws_config(remote):
    logger.info("Enter AWS Access Key: [!i]")
    access_key = input().strip()
    logger.info("Enter AWS Secret Key: [!i]")
    secret_key = input().strip()
    if not access_key or not secret_key:
        raise ValueError("Access Key and Secret Key must not be empty.")

    credentials = {
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region": remote.region,
        "provider": "AWS",
    }
    config = {"type": "s3", "parameters": credentials}
    return config


def _get_linode_config(remote):
    logger.info("Enter Linode Access Key: [!i]")
    access_key = input().strip()
    logger.info("Enter Linode Secret Key: [!i]")
    secret_key = input().strip()
    if not access_key or not secret_key:
        raise ValueError("Access Key and Secret Key must not be empty.")

    endpoint = f"{remote.region}.linodeobjects.com"
    credentials = {
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "provider": "Linode",
        "endpoint": endpoint,
    }
    config = {"type": "s3", "parameters": credentials}
    return config


def _get_check_remote_config(remote):
    bucket_path = remote.bucket_path if remote.bucket_path != "/" else ""
    remote_dir = f"{remote.bucket_name}{bucket_path}"
    return {
        "fs": f"{remote.name}:",
        "remote": remote_dir,
        "opt": {
            "recurse": False,
            "dirsOnly": True
        }
    }

class RCloneRemote:
    def __init__(self):
        self.name = None
        self.cloud = None
        self.bucket_name = None
        self.bucket_path = None
        self.region = None
        self.rc_config = None
        self.remote_config = None

    def create_remote(self,
                      rc_config: RcloneAPIConfig,
                      storage_config: dict,
                      bucket_side: str
                      ) -> None:
        self.cloud = storage_config.get("cloud")
        self.bucket_name = storage_config.get("bucket_name")
        bucket_path = storage_config.get("bucket_path", "/")
        self.bucket_path = bucket_path if bucket_path.endswith("/") else f"{bucket_path}/"
        self.region = storage_config.get("region", "")
        self.rc_config = rc_config

        logger.info(f"Please, provide credentials for the {bucket_side.upper()} bucket:")
        logger.info(f"  Name:   {self.bucket_name}")
        logger.info(f"  Path:   {self.bucket_path}")
        logger.info(f"  Cloud:  {self.cloud}")
        logger.info(f"  Region: {self.region}")
        self.remote_config = self._get_remote_config(self.cloud)

        self.name = f"{self.bucket_name}_{generate_random_string()}"
        self._send_create_request()
        self._check_remote_exists()
        logger.info("")

    def _send_create_request(self) -> None:
        self.remote_config["name"] = self.name
        base_url = self.rc_config.get_url()
        response = post_with_retries(
            f"{base_url}/config/create",
            self.remote_config,
            user=self.rc_config.user,
            password=self.rc_config.password
        )

        if not response or response.status_code != 200:
            raise RCloneRemoteException(
                f"Error creating remote connection to {self.bucket_name} ({self.cloud})."
            )

    def _check_remote_exists(self) -> None:
        data = _get_check_remote_config(self)
        base_url = self.rc_config.get_url()
        response = post_with_retries(
            f"{base_url}/operations/list",
            data,
            user=self.rc_config.user,
            password=self.rc_config.password
        )

        if not response or response.status_code != 200:
            self.close_remote()
            raise RCloneRemoteException(
                f"Error checking remote connection to {self.bucket_name} ({self.cloud})."
            )

    def _get_remote_config(self, cloud):
        if cloud == "azure":
            return _get_azure_config()
        elif cloud == "gcp":
            return _get_gcp_config(self)
        elif cloud == "aws":
            return _get_aws_config(self)
        elif cloud == "linode":
            return _get_linode_config(self)
        else:
            raise ValueError(
                "Unsupported cloud provider. Supported providers: azure, gcp, aws, linode."
            )

    def close_remote(self) -> None:
        data = {"name": self.name}
        base_url = self.rc_config.get_url()
        response = post_with_retries(
            f"{base_url}/config/delete",
            data,
            user=self.rc_config.user,
            password=self.rc_config.password
        )

        if response and response.status_code != 200:
            raise RCloneRemoteException(
                f"Error deleting remote connection for {self.bucket_name} ({self.cloud})."
            )

    def recreate_remote(self) -> None:
        self.close_remote()
        self._send_create_request()
        self._check_remote_exists()
