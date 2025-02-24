import random
import string
from copy import deepcopy
from typing import Optional, Tuple

from hdx_cli.cli_interface.migrate.helpers import confirm_action
from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.cli_interface.migrate.rc.rc_remotes import (
    RCloneRemote,
    get_check_remote_body,
    get_remote_config,
)
from hdx_cli.library_api.common.exceptions import (
    RCloneRemoteCheckException,
    RCloneRemoteException,
    StorageNotFoundError,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_storage_by_id

logger = get_logger()


def generate_random_string(length=5):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_remote(
    storage_settings: dict,
    remotes: dict,
    rc_config: RcloneAPIConfig,
    migration_side: str,
) -> RCloneRemote:
    cloud = storage_settings.get("cloud")
    bucket_name = storage_settings.get("bucket_name")
    bucket_path = storage_settings.get("bucket_path", "/")
    bucket_path = bucket_path if bucket_path.endswith("/") else f"{bucket_path}/"
    region = storage_settings.get("region", "")
    endpoint = storage_settings.get("endpoint", "")

    logger.info(f"Please, provide credentials for the {migration_side.upper()} bucket:")
    logger.info(f"  Name:     {bucket_name}")
    logger.info(f"  Path:     {bucket_path}")
    logger.info(f"  Cloud:    {cloud}")
    logger.info(f"  Region:   {region}")
    logger.info(f"  Endpoint: {endpoint}")

    new_remote, reused_remote = check_remotes_stock(
        remotes, bucket_name, bucket_path, cloud, region
    )
    if new_remote and reused_remote:
        logger.info("Access granted using a previous connection")
        logger.info(f"for bucket name: {reused_remote.bucket_name}{reused_remote.bucket_path}")
        logger.info("Reusing this connection may improve transfer speed.")
        logger.info("If you choose not to reuse it, credentials will be requested.")
        logger.info("")

        if confirm_action("Confirm reuse of this connection?"):
            logger.debug(
                f"Remote '{reused_remote.name}' is being reused for bucket: {storage_settings}"
            )
            logger.info("")
            return new_remote

    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        new_remote = RCloneRemote()
        new_remote.bucket_name = bucket_name
        new_remote.bucket_path = bucket_path
        new_remote.cloud = cloud
        new_remote.region = region
        new_remote.endpoint = endpoint
        new_remote.rc_config = rc_config
        new_remote.name = f"{bucket_name}_{generate_random_string()}"

        try:
            new_remote.remote_config = get_remote_config(new_remote)
            new_remote.remote_config["name"] = new_remote.name
            new_remote.create_remote()

            logger.info("Bucket connection successfully created")
            logger.info("")
            return new_remote
        except RCloneRemoteException as exc:
            logger.debug(f"Attempt {attempt + 1} failed with exception: {exc}")
            if isinstance(exc, RCloneRemoteCheckException):
                new_remote.close_remote()

            attempt += 1
            if attempt < max_retries:
                logger.info("There was an error during the bucket connection.")
                if confirm_action("Would you like to retry?"):
                    logger.info("")
                    continue
            logger.debug("Connection failed.")
            raise exc


def get_remote(
    remotes: dict,
    storages: list,
    storage_id: str,
    rc_config: RcloneAPIConfig,
    migration_side: str = "",
) -> RCloneRemote:
    if remote := remotes.get(storage_id):
        return remote

    storage_id, storage = get_storage_by_id(storages, storage_id)
    storage_settings = storage.get("settings")
    if not storage_settings:
        raise StorageNotFoundError(f"Storage UUID ({storage_id}) not found.")

    remote = create_remote(storage_settings, remotes, rc_config, migration_side)
    remotes[storage_id] = remote
    return remote


def close_remotes(remotes: dict) -> None:
    for remote in remotes.values():
        remote.close_remote()


def recreate_remotes(remotes: dict) -> None:
    for remote in remotes.values():
        remote.recreate_remote()


def check_existing_remote(remote: RCloneRemote, bucket_name, bucket_path) -> bool:
    bucket_info = get_check_remote_body(bucket_name, bucket_path, remote.name)
    try:
        remote.check_remote_exists(payload=bucket_info)
        return True
    except RCloneRemoteCheckException:
        return False


def check_remotes_stock(
    remotes: dict, bucket_name: str, bucket_path: str, cloud: str, region: str
) -> Tuple[Optional[RCloneRemote], Optional[RCloneRemote]]:
    for remote in remotes.values():
        assert isinstance(remote, RCloneRemote)
        if cloud != remote.cloud or region != remote.region:
            continue

        if check_existing_remote(remote, bucket_name, bucket_path):
            new_remote = deepcopy(remote)
            new_remote.bucket_name = bucket_name
            new_remote.bucket_path = bucket_path
            return new_remote, remote
    return None, None
