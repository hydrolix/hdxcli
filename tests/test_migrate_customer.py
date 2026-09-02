import pytest

from hdx_cli.cli_interface.migrate import customer as customer_module
from hdx_cli.cli_interface.migrate.customer import (
    collect_autoingest_credential_ids,
    collect_storage_map_ids,
    default_customer_candidate,
    find_customer,
    get_target_customer,
    project_requires_customer,
)
from hdx_cli.library_api.common.exceptions import ActionNotAvailableException


CUSTOMERS = [
    {"uuid": "aaaa-1111", "name": "hydro"},
    {"uuid": "bbbb-2222", "name": "sample_project"},
    {"uuid": "cccc-3333", "name": "acme"},
]


class TestFindCustomer:
    def test_finds_by_name(self):
        assert find_customer(CUSTOMERS, "acme")["uuid"] == "cccc-3333"

    def test_finds_by_uuid(self):
        assert find_customer(CUSTOMERS, "aaaa-1111")["name"] == "hydro"

    def test_returns_none_when_missing(self):
        assert find_customer(CUSTOMERS, "unknown") is None

    def test_empty_list(self):
        assert find_customer([], "acme") is None


class TestDefaultCustomerCandidate:
    def test_single_non_ci_customer_is_default(self):
        assert default_customer_candidate(CUSTOMERS)["name"] == "acme"

    def test_no_default_with_multiple_non_ci_customers(self):
        customers = CUSTOMERS + [{"uuid": "dddd-4444", "name": "globex"}]
        assert default_customer_candidate(customers) is None

    def test_no_default_with_only_ci_customers(self):
        assert default_customer_candidate(CUSTOMERS[:2]) is None


class TestCollectStorageMapIds:
    def test_collects_all_storage_map_sources(self):
        table = {
            "settings": {
                "storage_map": {
                    "default_storage_id": "st-1",
                    "column_value_mapping": {"st-2": ["1", "2"]},
                    "spread_list": ["st-3"],
                }
            }
        }
        assert collect_storage_map_ids(table) == {"st-1", "st-2", "st-3"}

    def test_empty_without_storage_map(self):
        assert collect_storage_map_ids({"settings": {}}) == set()
        assert collect_storage_map_ids({}) == set()

    def test_ignores_missing_keys(self):
        table = {"settings": {"storage_map": {"default_storage_id": "st-1"}}}
        assert collect_storage_map_ids(table) == {"st-1"}


class TestCollectAutoingestCredentialIds:
    def test_collects_both_credential_kinds(self):
        table = {
            "settings": {
                "autoingest": [
                    {"source_credential_id": "cr-1", "bucket_credential_id": "cr-2"},
                    {"source_credential_id": "cr-3"},
                ]
            }
        }
        assert collect_autoingest_credential_ids(table) == {"cr-1", "cr-2", "cr-3"}

    def test_empty_without_autoingest(self):
        assert collect_autoingest_credential_ids({"settings": {}}) == set()

    def test_ignores_non_dict_entries(self):
        table = {"settings": {"autoingest": ["bogus", {"source_credential_id": "cr-1"}]}}
        assert collect_autoingest_credential_ids(table) == {"cr-1"}


class TestProjectRequiresCustomer:
    def test_true_when_options_declare_customer(self, monkeypatch):
        monkeypatch.setattr(
            customer_module,
            "basic_options",
            lambda profile, path: {"name": {}, "customer": {"required": True}},
        )
        assert project_requires_customer(None, "/projects/") is True

    def test_false_when_options_lack_customer(self, monkeypatch):
        monkeypatch.setattr(customer_module, "basic_options", lambda profile, path: {"name": {}})
        assert project_requires_customer(None, "/projects/") is False

    def test_false_when_options_unavailable(self, monkeypatch):
        def raise_unavailable(profile, path):
            raise ActionNotAvailableException("no options")

        monkeypatch.setattr(customer_module, "basic_options", raise_unavailable)
        assert project_requires_customer(None, "/projects/") is False


class TestGetTargetCustomer:
    def test_none_when_target_does_not_require_customer(self, monkeypatch):
        monkeypatch.setattr(
            customer_module, "project_requires_customer", lambda profile, path: False
        )
        assert get_target_customer(None, "/projects/", "acme") is None

    def test_option_resolves_by_name(self, monkeypatch):
        monkeypatch.setattr(
            customer_module, "project_requires_customer", lambda profile, path: True
        )
        monkeypatch.setattr(customer_module, "list_customers", lambda profile: CUSTOMERS)
        customer = get_target_customer(None, "/projects/", "acme")
        assert customer["uuid"] == "cccc-3333"

    def test_existing_project_customer_wins(self, monkeypatch):
        monkeypatch.setattr(customer_module, "list_customers", lambda profile: CUSTOMERS)
        existing_project = {"name": "proj", "customer": "cccc-3333"}
        customer = get_target_customer(
            None, "/projects/", "hydro", existing_project=existing_project
        )
        assert customer["name"] == "acme"

    def test_existing_project_without_customer_yields_none(self, monkeypatch):
        existing_project = {"name": "proj"}
        assert (
            get_target_customer(None, "/projects/", None, existing_project=existing_project) is None
        )

    def test_existing_project_with_unknown_customer_keeps_uuid(self, monkeypatch):
        monkeypatch.setattr(customer_module, "list_customers", lambda profile: [])
        existing_project = {"name": "proj", "customer": "zzzz-9999"}
        customer = get_target_customer(None, "/projects/", None, existing_project=existing_project)
        assert customer == {"uuid": "zzzz-9999", "name": "zzzz-9999"}
