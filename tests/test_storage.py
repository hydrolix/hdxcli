import pytest

from hdx_cli.library_api.common.storage import is_same_bucket


class TestIsSameBucket:
    def test_matching_aws_storages(self):
        source = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-east-1",
            "cloud": "aws",
        }
        target = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-east-1",
            "cloud": "aws",
        }
        assert is_same_bucket(source, target) is True

    def test_different_region_aws_storages(self):
        source = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-east-1",
            "cloud": "aws",
        }
        target = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-west-2",
            "cloud": "aws",
        }
        assert is_same_bucket(source, target) is False

    def test_azure_same_bucket_region_mismatch(self):
        """Azure storage equivalence should ignore region differences (HDX-10682)."""
        source = {
            "bucket_name": "my-container",
            "bucket_path": "/data",
            "region": "japanwest",
            "cloud": "azure",
        }
        target = {
            "bucket_name": "my-container",
            "bucket_path": "/data",
            "cloud": "azure",
        }
        assert is_same_bucket(source, target) is True

    def test_azure_same_bucket_both_have_region(self):
        """Azure storages match even when both have (different) regions."""
        source = {
            "bucket_name": "my-container",
            "bucket_path": "/data",
            "region": "japanwest",
            "cloud": "azure",
        }
        target = {
            "bucket_name": "my-container",
            "bucket_path": "/data",
            "region": "japaneast",
            "cloud": "azure",
        }
        assert is_same_bucket(source, target) is True

    def test_azure_different_bucket(self):
        source = {
            "bucket_name": "container-a",
            "bucket_path": "/data",
            "cloud": "azure",
        }
        target = {
            "bucket_name": "container-b",
            "bucket_path": "/data",
            "cloud": "azure",
        }
        assert is_same_bucket(source, target) is False

    def test_different_cloud_providers(self):
        source = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-east-1",
            "cloud": "aws",
        }
        target = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-east-1",
            "cloud": "gcp",
        }
        assert is_same_bucket(source, target) is False

    def test_gcp_different_region_fails(self):
        source = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "us-central1",
            "cloud": "gcp",
        }
        target = {
            "bucket_name": "my-bucket",
            "bucket_path": "/data",
            "region": "europe-west1",
            "cloud": "gcp",
        }
        assert is_same_bucket(source, target) is False
