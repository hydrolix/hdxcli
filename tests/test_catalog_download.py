from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from hdx_cli.cli_interface.migrate.catalog_operations import Catalog


FAKE_CSV = (
    b"created,modified,min_timestamp,max_timestamp,manifest_size,data_size,"
    b"index_size,root_path,data_path,active,rows,mem_size,metadata,shard_key,lock,storage_id\n"
    b"2026-03-14 23:50:00,2026-03-14 23:50:00,2026-03-14 23:50:00,2026-03-14 23:55:00,"
    b"100,200,50,proj/tbl,part1,true,1000,0,{},,,storage-1\n"
)


def _make_profile(scheme="https", hostname="cluster.example.com", org_id="org-1"):
    profile = MagicMock()
    profile.scheme = scheme
    profile.hostname = hostname
    profile.org_id = org_id
    profile.auth.token_type = "Bearer"
    profile.auth.token = "tok"
    return profile


class TestCatalogDownloadDateFiltering:
    def test_no_dates_omits_timestamp_params(self):
        """Without dates the URL should contain no timestamp query params."""
        profile = _make_profile()
        captured = {}

        def fake_get(url, **kwargs):
            captured["url"] = url
            return FAKE_CSV

        with patch("hdx_cli.cli_interface.migrate.catalog_operations.rest_ops.get", fake_get):
            catalog = Catalog()
            catalog.download(profile, "proj-id", "tbl-id")

        assert "min_timestamp_after" not in captured["url"]
        assert "max_timestamp_before" not in captured["url"]

    def test_from_date_appends_min_timestamp_after(self):
        """from_date should appear as min_timestamp_after in ISO-8601 format."""
        profile = _make_profile()
        captured = {}

        def fake_get(url, **kwargs):
            captured["url"] = url
            return FAKE_CSV

        from_date = datetime(2026, 3, 14, 23, 50, 0)
        with patch("hdx_cli.cli_interface.migrate.catalog_operations.rest_ops.get", fake_get):
            catalog = Catalog()
            catalog.download(profile, "proj-id", "tbl-id", from_date=from_date)

        assert "min_timestamp_after=2026-03-14T23%3A50%3A00Z" in captured["url"] or \
               "min_timestamp_after=2026-03-14T23:50:00Z" in captured["url"]

    def test_to_date_appends_max_timestamp_before(self):
        """to_date should appear as max_timestamp_before in ISO-8601 format."""
        profile = _make_profile()
        captured = {}

        def fake_get(url, **kwargs):
            captured["url"] = url
            return FAKE_CSV

        to_date = datetime(2026, 3, 15, 0, 0, 0)
        with patch("hdx_cli.cli_interface.migrate.catalog_operations.rest_ops.get", fake_get):
            catalog = Catalog()
            catalog.download(profile, "proj-id", "tbl-id", to_date=to_date)

        assert "max_timestamp_before=2026-03-15T00%3A00%3A00Z" in captured["url"] or \
               "max_timestamp_before=2026-03-15T00:00:00Z" in captured["url"]

    def test_both_dates_appended(self):
        """Both params appear when both dates are provided."""
        profile = _make_profile()
        captured = {}

        def fake_get(url, **kwargs):
            captured["url"] = url
            return FAKE_CSV

        from_date = datetime(2026, 3, 14, 23, 50, 0)
        to_date = datetime(2026, 3, 15, 0, 0, 0)
        with patch("hdx_cli.cli_interface.migrate.catalog_operations.rest_ops.get", fake_get):
            catalog = Catalog()
            catalog.download(profile, "proj-id", "tbl-id", from_date=from_date, to_date=to_date)

        url = captured["url"]
        assert "min_timestamp_after=" in url
        assert "max_timestamp_before=" in url
