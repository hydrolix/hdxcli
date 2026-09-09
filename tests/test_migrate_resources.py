from types import SimpleNamespace

from hdx_cli.cli_interface.migrate import resources as resources_module


def _run_create_project(monkeypatch, source_body, customer):
    """Call _create_project with the adapter/normalizer/HTTP layer stubbed out,
    returning the body that would have been POSTed to the target."""
    captured = {}

    # Pass the body through the adapter and normalizer unchanged so we can
    # assert on exactly what _create_project prepared.
    monkeypatch.setattr(
        resources_module, "adapt_resource_to_api_structure", lambda profile, path, body: body
    )
    monkeypatch.setattr(resources_module, "normalize_project", lambda body, reuse_partitions: body)

    def fake_basic_create(profile, path, name, *, body):
        captured["body"] = body

    monkeypatch.setattr(resources_module, "basic_create", fake_basic_create)

    profile = SimpleNamespace(projectname="target_proj")
    resources_module._create_project(
        profile, source_body, "/config/v1/orgs/o/projects/", customer, False
    )
    return captured["body"]


class TestCreateProjectBody:
    def test_strips_cross_cluster_identity_fields(self, monkeypatch):
        source_body = {
            "name": "source_proj",
            "customer": "source-customer-uuid",
            "org": "source-org-uuid",
            "hdx_deployment_id": "ns__source_proj",
            "description": "keep me",
        }
        body = _run_create_project(monkeypatch, source_body, customer=None)
        assert "customer" not in body
        assert "org" not in body
        assert "hdx_deployment_id" not in body
        assert body["description"] == "keep me"

    def test_injects_resolved_customer(self, monkeypatch):
        source_body = {"name": "source_proj", "customer": "source-customer-uuid"}
        body = _run_create_project(
            monkeypatch, source_body, customer={"uuid": "target-customer-uuid", "name": "acme"}
        )
        assert body["customer"] == "target-customer-uuid"
