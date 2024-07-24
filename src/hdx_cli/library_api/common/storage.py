from typing import Union, Tuple


def _is_same_bucket(settings_source: dict, settings_target: dict) -> bool:
    result = False
    if (
            settings_source.get('bucket_name') == settings_target.get('bucket_name') and
            settings_source.get('bucket_path') == settings_target.get('bucket_path') and
            settings_source.get('region') == settings_target.get('region') and
            settings_source.get('cloud') == settings_target.get('cloud')
    ):
        result = True
    return result


def _look_for_same_bucket(settings: dict, storages: list[dict]) -> Union[str, None]:
    for storage in storages:
        if _is_same_bucket(settings, storage.get('settings')):
            return storage.get('uuid')
    return None


def equivalent_storages(source_storages: list[dict], target_storages: list[dict]) -> dict[str, str]:
    result_dict = {}
    for source_storage in source_storages:
        source_storage_settings = source_storage.get('settings')
        target_storage_uuid = _look_for_same_bucket(source_storage_settings, target_storages)
        if target_storage_uuid:
            result_dict[source_storage.get('uuid')] = target_storage_uuid

            # Default source storage added to map it when null storage_id values
            # exist in catalog records
            if source_storage_settings.get('is_default'):
                result_dict['default'] = target_storage_uuid

    return result_dict


def get_storage_by_id(storages: list[dict], storage_id: str) -> Union[Tuple[str, dict], None]:
    for storage in storages:
        if storage.get('uuid') == storage_id:
            return storage_id, storage.get('settings')
    return None


def get_storage_default(storages: list[dict]) -> Union[Tuple[str, dict], None]:
    for storage in storages:
        if storage.get('settings', {}).get('is_default'):
            return storage.get('uuid'), storage.get('settings')
    return None
