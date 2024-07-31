from .exceptions import ProviderClassNotFoundError, StorageNotFoundError
from ..provider import BaseProvider, AwsProvider, AzureProvider, GcpProvider, LinodeProvider
from .storage import get_storage_by_id, get_storage_default


def setup_provider(storage_settings: dict, credentials=None) -> BaseProvider:
    if credentials is None:
        credentials = {}

    cloud = storage_settings.get('cloud').strip()
    region = storage_settings.get('region').strip()
    endpoint = storage_settings.get('endpoint')
    # For now this is necessary because cloud information is not always used as expected
    if cloud == 'aws' and endpoint is not None and 'linode' in endpoint:
        cloud = 'linode'
    bucket_name = storage_settings.get('bucket_name').strip()
    bucket_path = storage_settings.get('bucket_path').strip()
    provider_class_name = cloud.capitalize() + 'Provider'

    if provider_class_name in globals():
        provider_class = globals()[provider_class_name]
        return provider_class(bucket_name, bucket_path, region).setup_connection_to_bucket(**credentials)

    raise ProviderClassNotFoundError(f"Provider '{provider_class_name}' not found.")


def get_provider(providers: dict, storages: list, storage_id=None, credentials=None) -> BaseProvider:
    if credentials is None:
        credentials = {}

    storage_id, storage_settings = get_storage_by_id(storages, storage_id) if storage_id else get_storage_default(storages)
    if provider := providers.get(storage_id):
        return provider

    if not storage_settings:
        raise StorageNotFoundError(f"Storage id ({storage_id}) not found.")

    provider = setup_provider(storage_settings, credentials)
    providers[storage_id] = provider
    return provider
