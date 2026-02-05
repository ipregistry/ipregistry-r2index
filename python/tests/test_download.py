"""Tests for download functionality."""

from pathlib import Path
from unittest.mock import patch

import pytest
from pytest_httpx import HTTPXMock

from elaunira.r2index import (
    R2IndexClient,
    RemoteTuple,
)
from elaunira.r2index.storage import R2TransferConfig


class TestGetByTuple:
    """Tests for get_by_tuple method."""

    @pytest.fixture
    def client(self):
        """Create a test client."""
        return R2IndexClient(
            index_api_url="https://api.example.com",
            index_api_token="test-token",
        )

    def test_get_by_tuple(self, client: R2IndexClient, httpx_mock: HTTPXMock):
        """Test getting a file by remote tuple."""
        httpx_mock.add_response(
            url="https://api.example.com/files/by-tuple?bucket=test-bucket&remote_path=%2Freleases%2Fmyapp&remote_filename=myapp.zip&remote_version=v1",
            json={
                "id": "file123",
                "bucket": "test-bucket",
                "category": "software",
                "entity": "myapp",
                "extension": "zip",
                "media_type": "application/zip",
                "remote_path": "/releases/myapp",
                "remote_filename": "myapp.zip",
                "remote_version": "v1",
                "tags": [],
                "size": 1024,
                "checksum_md5": "abc",
                "checksum_sha1": "def",
                "checksum_sha256": "ghi",
                "checksum_sha512": "jkl",
                "created": 1704067200,
                "updated": 1704067200,
            },
        )

        remote_tuple = RemoteTuple(
            bucket="test-bucket",
            remote_path="/releases/myapp",
            remote_filename="myapp.zip",
            remote_version="v1",
        )
        record = client.get_by_tuple(remote_tuple)

        assert record.id == "file123"
        assert record.bucket == "test-bucket"
        assert record.remote_path == "/releases/myapp"
        assert record.remote_filename == "myapp.zip"
        assert record.remote_version == "v1"


class TestDownload:
    """Tests for download method."""

    @pytest.fixture
    def client_with_r2(self):
        """Create a test client with R2 config."""
        return R2IndexClient(
            index_api_url="https://api.example.com",
            index_api_token="test-token",
            r2_access_key_id="test-key",
            r2_secret_access_key="test-secret",
            r2_endpoint_url="https://r2.example.com",
        )

    def test_download_with_defaults(
        self, client_with_r2: R2IndexClient, httpx_mock: HTTPXMock, tmp_path: Path
    ):
        """Test download with default IP and user agent."""
        # Mock checkip.amazonaws.com
        httpx_mock.add_response(
            url="https://checkip.amazonaws.com",
            text="203.0.113.1\n",
        )

        # Mock get_by_tuple
        httpx_mock.add_response(
            url="https://api.example.com/files/by-tuple?bucket=test-bucket&remote_path=%2Freleases%2Fmyapp&remote_filename=myapp.zip&remote_version=v1",
            json={
                "id": "file123",
                "bucket": "test-bucket",
                "category": "software",
                "entity": "myapp",
                "extension": "zip",
                "media_type": "application/zip",
                "remote_path": "/releases/myapp",
                "remote_filename": "myapp.zip",
                "remote_version": "v1",
                "tags": [],
                "size": 1024,
                "checksum_md5": "abc",
                "checksum_sha1": "def",
                "checksum_sha256": "ghi",
                "checksum_sha512": "jkl",
                "created": 1704067200,
                "updated": 1704067200,
            },
        )

        # Mock record_download
        httpx_mock.add_response(
            url="https://api.example.com/downloads",
            method="POST",
            status_code=201,
            json={
                "id": "download123",
                "bucket": "test-bucket",
                "remote_path": "/releases/myapp",
                "remote_filename": "myapp.zip",
                "remote_version": "v1",
                "ip_address": "203.0.113.1",
                "user_agent": "elaunira-r2index/0.1.0",
                "downloaded_at": 1704067200,
            },
        )

        destination = tmp_path / "myapp.zip"

        # Mock the R2 storage download
        with patch.object(
            client_with_r2._get_storage(),
            "download_file",
            return_value=destination,
        ) as mock_download:
            downloaded_path, file_record = client_with_r2.download(
                bucket="test-bucket",
                source_path="/releases/myapp",
                source_filename="myapp.zip",
                source_version="v1",
                destination=str(destination),
            )

            mock_download.assert_called_once()
            assert downloaded_path == destination
            assert file_record.id == "file123"

    def test_download_with_explicit_ip_and_user_agent(
        self, client_with_r2: R2IndexClient, httpx_mock: HTTPXMock, tmp_path: Path
    ):
        """Test download with explicit IP and user agent."""
        # Mock get_by_tuple
        httpx_mock.add_response(
            url="https://api.example.com/files/by-tuple?bucket=test-bucket&remote_path=%2Freleases%2Fmyapp&remote_filename=myapp.zip&remote_version=v1",
            json={
                "id": "file123",
                "bucket": "test-bucket",
                "category": "software",
                "entity": "myapp",
                "extension": "zip",
                "media_type": "application/zip",
                "remote_path": "/releases/myapp",
                "remote_filename": "myapp.zip",
                "remote_version": "v1",
                "tags": [],
                "size": 1024,
                "checksum_md5": "abc",
                "checksum_sha1": "def",
                "checksum_sha256": "ghi",
                "checksum_sha512": "jkl",
                "created": 1704067200,
                "updated": 1704067200,
            },
        )

        # Mock record_download
        httpx_mock.add_response(
            url="https://api.example.com/downloads",
            method="POST",
            status_code=201,
            json={
                "id": "download123",
                "bucket": "test-bucket",
                "remote_path": "/releases/myapp",
                "remote_filename": "myapp.zip",
                "remote_version": "v1",
                "ip_address": "10.0.0.1",
                "user_agent": "custom-agent/1.0",
                "downloaded_at": 1704067200,
            },
        )

        destination = tmp_path / "myapp.zip"

        # Mock the R2 storage download
        with patch.object(
            client_with_r2._get_storage(),
            "download_file",
            return_value=destination,
        ):
            downloaded_path, file_record = client_with_r2.download(
                bucket="test-bucket",
                source_path="/releases/myapp",
                source_filename="myapp.zip",
                source_version="v1",
                destination=str(destination),
                ip_address="10.0.0.1",
                user_agent="custom-agent/1.0",
            )

            assert downloaded_path == destination
            assert file_record.id == "file123"


class TestR2TransferConfig:
    """Tests for R2TransferConfig."""

    def test_default_values(self):
        """Test default transfer config values."""
        config = R2TransferConfig()
        assert config.multipart_threshold == 100 * 1024 * 1024  # 100MB
        assert config.multipart_chunksize == 100 * 1024 * 1024  # 100MB
        assert config.max_concurrency >= 4  # At least 4
        assert config.use_threads is True

    def test_custom_values(self):
        """Test custom transfer config values."""
        config = R2TransferConfig(
            multipart_threshold=50 * 1024 * 1024,
            multipart_chunksize=25 * 1024 * 1024,
            max_concurrency=8,
            use_threads=False,
        )
        assert config.multipart_threshold == 50 * 1024 * 1024
        assert config.multipart_chunksize == 25 * 1024 * 1024
        assert config.max_concurrency == 8
        assert config.use_threads is False
