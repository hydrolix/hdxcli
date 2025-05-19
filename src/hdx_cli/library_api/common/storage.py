from typing import Optional, Tuple, Union

from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.models import ProfileUserContext


def is_same_bucket(settings_source: dict, settings_target: dict) -> bool:
    result = False
    if (
        settings_source.get("bucket_name") == settings_target.get("bucket_name")
        and settings_source.get("bucket_path") == settings_target.get("bucket_path")
        and settings_source.get("region") == settings_target.get("region")
        and settings_source.get("cloud") == settings_target.get("cloud")
    ):
        result = True
    return result


def look_for_same_bucket(settings: dict, storages: list[dict]) -> Union[str, None]:
    for storage in storages:
        if is_same_bucket(settings, storage.get("settings")):
            return storage.get("uuid")
    return None


def get_equivalent_storages(
    source_storages: list[dict], target_storages: list[dict]
) -> dict[str, str]:
    result_dict = {}
    for source_storage in source_storages:
        source_storage_settings = source_storage.get("settings")
        target_storage_uuid = look_for_same_bucket(source_storage_settings, target_storages)
        if target_storage_uuid:
            result_dict[source_storage.get("uuid")] = target_storage_uuid

            # Default source storage added to map it when null storage_id values
            # exist in catalog records
            if source_storage_settings.get("is_default"):
                result_dict["default"] = target_storage_uuid
    return result_dict


def get_storage_default_by_table(profile: ProfileUserContext, storages: list) -> str:
    table, _ = access_resource_detailed(
        profile, [("projects", profile.projectname), ("tables", profile.tablename)]
    )

    table_storage_map = table.get("settings", {}).get("storage_map", {})
    table_default_storage_id = table_storage_map.get("default_storage_id", None)

    if not table_default_storage_id:
        table_default_storage_id, _ = get_storage_default(storages)
    return table_default_storage_id


def get_storage_by_id(storages: list[dict], storage_id: str) -> Tuple[str, Optional[dict]]:
    for storage in storages:
        if storage.get("uuid") == storage_id:
            return storage_id, storage
    return storage_id, None


def get_storage_default(storages: list[dict]) -> Tuple[Optional[str], Optional[dict]]:
    for storage in storages:
        if storage.get("settings", {}).get("is_default"):
            return storage.get("uuid"), storage.get("settings")
    return None, None


def valid_storage_id(storage_id: str, storages: list[dict]) -> bool:
    if not storage_id or storage_id not in [storage.get("uuid") for storage in storages]:
        return False
    return True
