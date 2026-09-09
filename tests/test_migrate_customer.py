import pytest

from hdx_cli.cli_interface.migrate import customer as customer_module
from hdx_cli.cli_interface.migrate.customer import (
    collect_autoingest_credential_ids,
    collect_storage_map_ids,
    customer_field_status,
    default_customer_candidate,
    find_customer,
    get_target_customer,
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

    def test_matches_slugified_name(self):
        # The server stores "Acme Corp" as "Acme-Corp"; typing the spaced form
        # must still resolve to it.
        customers = [{"uuid": "dddd-4444", "name": "Acme-Corp"}]
        assert find_customer(customers, "Acme Corp")["uuid"] == "dddd-4444"


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


class TestCustomerFieldStatus:
    def test_required_when_options_mark_it_required(self, monkeypatch):
        # 6.4+: field present and required.
        monkeypatch.setattr(
            customer_module,
            "basic_options",
            lambda profile, path: {"name": {}, "customer": {"required": True}},
        )
        assert customer_field_status(None, "/projects/") == (True, True)

    def test_supported_but_optional(self, monkeypatch):
        # 6.1-6.3.x: field present but optional (server auto-assigns default).
        monkeypatch.setattr(
            customer_module,
            "basic_options",
            lambda profile, path: {"name": {}, "customer": {"required": False}},
        )
        assert customer_field_status(None, "/projects/") == (True, False)

    def test_unsupported_when_options_lack_customer(self, monkeypatch):
        # Pre-6.1: no customer field at all.
        monkeypatch.setattr(customer_module, "basic_options", lambda profile, path: {"name": {}})
        assert customer_field_status(None, "/projects/") == (False, False)

    def test_unsupported_when_options_unavailable(self, monkeypatch):
        def raise_unavailable(profile, path):
            raise ActionNotAvailableException("no options")

        monkeypatch.setattr(customer_module, "basic_options", raise_unavailable)
        assert customer_field_status(None, "/projects/") == (False, False)


class TestGetTargetCustomer:
    def test_none_when_target_does_not_support_customer(self, monkeypatch):
        monkeypatch.setattr(
            customer_module, "customer_field_status", lambda profile, path: (False, False)
        )
        assert get_target_customer(None, "/projects/", "acme") is None

    def test_none_when_optional_and_no_flag(self, monkeypatch):
        # Regression guard: an optional-field cluster (6.1-6.3.x) with no
        # --target-customer must NOT drop into the interactive picker; the
        # server auto-assigns the default.
        monkeypatch.setattr(
            customer_module, "customer_field_status", lambda profile, path: (True, False)
        )
        # list_customers/input must never be reached; leave them unpatched so a
        # network/EOF error would fail the test loudly.
        assert get_target_customer(None, "/projects/", None) is None

    def test_flag_honored_when_optional(self, monkeypatch):
        # An explicit --target-customer is honored even when the field is
        # optional (settable on 6.1-6.3.x).
        monkeypatch.setattr(
            customer_module, "customer_field_status", lambda profile, path: (True, False)
        )
        monkeypatch.setattr(customer_module, "list_customers", lambda profile: CUSTOMERS)
        customer = get_target_customer(None, "/projects/", "acme")
        assert customer["uuid"] == "cccc-3333"

    def test_option_resolves_by_name_when_required(self, monkeypatch):
        monkeypatch.setattr(
            customer_module, "customer_field_status", lambda profile, path: (True, True)
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


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class TestCreateCustomer:
    def test_uses_response_body_with_slugified_name(self, monkeypatch):
        # Server slugifies "Acme Corp" -> "Acme-Corp" and returns the real
        # object; _create_customer must trust that, not re-read by typed name.
        monkeypatch.setattr(
            customer_module,
            "basic_create",
            lambda *a, **k: _FakeResponse({"uuid": "new-1", "name": "Acme-Corp"}),
        )
        result = customer_module._create_customer(None, "Acme Corp")
        assert result == {"uuid": "new-1", "name": "Acme-Corp"}

    def test_raises_when_response_has_no_uuid(self, monkeypatch):
        from hdx_cli.library_api.common.exceptions import HdxCliException

        monkeypatch.setattr(customer_module, "basic_create", lambda *a, **k: _FakeResponse({}))
        with pytest.raises(HdxCliException):
            customer_module._create_customer(None, "acme")
