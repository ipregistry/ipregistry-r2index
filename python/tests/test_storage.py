"""Tests for storage-level features: progress callback, format_bytes, overwrite."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from elaunira.r2index.storage import R2Config, R2Storage, _ProgressCallback, _format_bytes


class TestFormatBytes:
    """Tests for _format_bytes helper."""

    def test_bytes(self):
        assert _format_bytes(0) == "0.00 B"
        assert _format_bytes(512) == "512.00 B"

    def test_kilobytes(self):
        assert _format_bytes(1024) == "1.00 KB"
        assert _format_bytes(1536) == "1.50 KB"

    def test_megabytes(self):
        assert _format_bytes(1024 * 1024) == "1.00 MB"
        assert _format_bytes(150 * 1024 * 1024) == "150.00 MB"

    def test_gigabytes(self):
        assert _format_bytes(1024**3) == "1.00 GB"
        assert _format_bytes(3.5 * 1024**3) == "3.50 GB"

    def test_terabytes(self):
        assert _format_bytes(1024**4) == "1.00 TB"


class TestProgressCallback:
    """Tests for _ProgressCallback."""

    def test_accumulates_bytes(self):
        cb = _ProgressCallback(None)
        cb(100)
        cb(200)
        assert cb._bytes_transferred == 300

    def test_invokes_user_callback_with_cumulative(self):
        user_cb = MagicMock()
        cb = _ProgressCallback(user_cb)
        cb(100)
        cb(200)
        user_cb.assert_any_call(100)
        user_cb.assert_any_call(300)

    def test_no_logging_when_interval_is_none(self, caplog):
        cb = _ProgressCallback(None, progress_interval=None)
        with caplog.at_level(logging.INFO):
            cb(1024)
        assert caplog.text == ""

    def test_logs_when_interval_elapsed(self, caplog):
        cb = _ProgressCallback(
            None,
            total_size=1024 * 1024,
            progress_interval=0.0,  # log on every call
            operation="Downloading",
        )
        with caplog.at_level(logging.INFO):
            cb(512 * 1024)
        assert "Downloading" in caplog.text
        assert "50.0%" in caplog.text

    def test_logs_without_total_size(self, caplog):
        cb = _ProgressCallback(
            None,
            total_size=None,
            progress_interval=0.0,
            operation="Uploading",
        )
        with caplog.at_level(logging.INFO):
            cb(1024)
        assert "Uploading" in caplog.text
        assert "%" not in caplog.text


class TestDownloadOverwrite:
    """Tests for the overwrite parameter on R2Storage.download_file."""

    @pytest.fixture
    def storage(self):
        config = R2Config(
            access_key_id="key",
            secret_access_key="secret",
            endpoint_url="https://r2.example.com",
        )
        return R2Storage(config)

    def test_skip_when_overwrite_false_and_file_exists(self, storage, tmp_path):
        existing = tmp_path / "existing.txt"
        existing.write_text("data")

        with patch.object(storage, "_client") as mock_client:
            result = storage.download_file(
                bucket="b",
                object_key="key",
                file_path=existing,
                overwrite=False,
            )

            mock_client.download_file.assert_not_called()
            assert result == existing

    def test_download_when_overwrite_false_and_file_missing(self, storage, tmp_path):
        dest = tmp_path / "new.txt"

        with patch.object(storage, "_client") as mock_client:
            mock_client.head_object.return_value = {"ContentLength": 100}
            storage.download_file(
                bucket="b",
                object_key="key",
                file_path=dest,
                overwrite=False,
            )

            mock_client.download_file.assert_called_once()

    def test_download_when_overwrite_true_and_file_exists(self, storage, tmp_path):
        existing = tmp_path / "existing.txt"
        existing.write_text("data")

        with patch.object(storage, "_client") as mock_client:
            mock_client.head_object.return_value = {"ContentLength": 100}
            storage.download_file(
                bucket="b",
                object_key="key",
                file_path=existing,
                overwrite=True,
            )

            mock_client.download_file.assert_called_once()
