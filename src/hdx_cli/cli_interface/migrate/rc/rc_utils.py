from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.cli_interface.migrate.rc.rc_remotes import RCloneRemote
from hdx_cli.library_api.common.exceptions import StorageNotFoundError
from hdx_cli.library_api.common.storage import get_storage_by_id


def get_remote(remotes: dict,
			   storages: list,
			   storage_id: str,
			   rc_config: RcloneAPIConfig,
               bucket_side: str = ""
			   ) -> RCloneRemote:
    if remote := remotes.get(storage_id):
        return remote

    storage_id, storage_settings = get_storage_by_id(storages, storage_id)
    if not storage_settings:
        raise StorageNotFoundError(f"Storage UUID ({storage_id}) not found.")

    remote = RCloneRemote()
    remote.create_remote(rc_config, storage_settings, bucket_side)
    remotes[storage_id] = remote
    return remote


def close_remotes(remotes: dict) -> None:
    for remote in remotes.values():
        remote.close_remote()


def recreate_remotes(remotes: dict) -> None:
    for remote in remotes.values():
        remote.recreate_remote()
