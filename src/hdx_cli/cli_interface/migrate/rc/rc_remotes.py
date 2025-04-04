import json
import os
import random
import string

from hdx_cli.library_api.common.exceptions import (
    RCloneRemoteCheckException,
    RCloneRemoteCreationException,
    RCloneRemoteException,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.rest_operations import post_with_retries

logger = get_logger()


def generate_random_string(length=5):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _get_azure_config():
    logger.info("Enter Azure account: [!i]")
    account = input().strip()
    logger.info("Enter Azure key: [!i]")
    key = input().strip()

    credentials = {"account": account, "key": key}
    config = {"type": "azureblob", "parameters": credentials}
    return config


def _get_gcp_config(remote):
    logger.info("Enter path to Google Service Account JSON file: [!i]")
    json_path = input().strip()
    if not os.path.isfile(json_path):
        raise ValueError("Invalid path for Google Service Account JSON file.")

    with open(json_path, "r", encoding="utf-8") as file:
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

    if not remote.endpoint:
        remote.endpoint = f"{remote.region}.linodeobjects.com"
    credentials = {
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "provider": "Linode",
        "endpoint": remote.endpoint,
    }
    config = {"type": "s3", "parameters": credentials}
    return config


def get_check_remote_body(bucket_name: str, bucket_path: str, remote_name: str) -> dict:
    bucket_path = bucket_path if bucket_path != "/" else ""
    remote_dir = f"{bucket_name}{bucket_path}"
    return {
        "fs": f"{remote_name}:",
        "remote": remote_dir,
        "opt": {"recurse": False, "dirsOnly": True},
    }


def get_remote_config(remote):
    if remote.cloud == "azure":
        return _get_azure_config()
    if remote.cloud == "gcp":
        return _get_gcp_config(remote)
    if remote.cloud in ["aws", "linode"]:
        if remote.endpoint or remote.cloud == "linode":
            return _get_linode_config(remote)
        return _get_aws_config(remote)
    raise ValueError("Unsupported cloud provider. Supported providers: azure, gcp, aws, linode.")


class RCloneRemote:
    def __init__(self):
        self.name = None
        self.cloud = None
        self.bucket_name = None
        self.bucket_path = None
        self.region = None
        self.endpoint = None
        self.rc_config = None
        self.remote_config = None

    def create_remote(self) -> None:
        self._send_create_request()
        self.check_remote_exists()

    def _send_create_request(self) -> None:
        base_url = self.rc_config.get_url()
        auth = (self.rc_config.user, self.rc_config.password)
        url = f"{base_url}/config/create"
        response = post_with_retries(url, body=self.remote_config, auth=auth)

        if not response or response.status_code != 200:
            raise RCloneRemoteCreationException(self.bucket_name, self.cloud)

    def check_remote_exists(self, payload=None) -> None:
        if not payload:
            payload = get_check_remote_body(self.bucket_name, self.bucket_path, self.name)
        base_url = self.rc_config.get_url()
        response = post_with_retries(
            f"{base_url}/operations/list",
            body=payload,
            auth=(self.rc_config.user, self.rc_config.password),
        )

        if not response or response.status_code != 200:
            raise RCloneRemoteCheckException(self.bucket_name, self.cloud)

    def close_remote(self) -> None:
        data = {"name": self.name}
        base_url = self.rc_config.get_url()
        response = post_with_retries(
            f"{base_url}/config/delete",
            body=data,
            auth=(self.rc_config.user, self.rc_config.password),
        )

        if response and response.status_code != 200:
            raise RCloneRemoteException(
                f"Error deleting remote connection for {self.bucket_name} ({self.cloud})."
            )

    def recreate_remote(self) -> None:
        self.close_remote()
        self._send_create_request()
        self.check_remote_exists()
