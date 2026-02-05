#!/usr/bin/env python3
"""End-to-end test for r2index API using the Python client library.

Usage:
    python e2e_test.py <api_url> <api_token>

Example:
    python e2e_test.py https://r2index.example.com my-secret-token
"""

import sys
import time
from datetime import datetime, timedelta, timezone

from elaunira.r2index import (
    R2IndexClient,
    FileCreateRequest,
    FileUpdateRequest,
    RemoteTuple,
    NotFoundError,
    ValidationError,
    AuthenticationError,
)


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"


class E2ETest:
    def __init__(self, api_url: str, api_token: str):
        self.api_url = api_url
        self.api_token = api_token
        self.passed = 0
        self.failed = 0
        self.client = R2IndexClient(index_api_url=api_url, index_api_token=api_token)

    def pass_test(self, name: str) -> None:
        print(f"{Colors.GREEN}✓ {name}{Colors.RESET}")
        self.passed += 1

    def fail_test(self, name: str, error: str = "") -> None:
        print(f"{Colors.RED}✗ {name}{Colors.RESET}")
        if error:
            print(f"{Colors.RED}  Error: {error}{Colors.RESET}")
        self.failed += 1

    def section(self, name: str) -> None:
        print(f"\n{Colors.YELLOW}=== {name} ==={Colors.RESET}")

    def run(self) -> bool:
        print("=" * 50)
        print("R2Index API End-to-End Test (Python Client)")
        print("=" * 50)
        print(f"API URL: {self.api_url}")
        print("=" * 50)

        try:
            self.test_health()
            self.test_authentication()
            file_id = self.test_file_crud()
            self.test_download_tracking()
            self.test_analytics()
            self.test_file_deletion(file_id)
            self.test_error_handling()
            self.test_maintenance()
        finally:
            self.client.close()

        # Print summary
        print()
        print("=" * 50)
        print("Test Results")
        print("=" * 50)
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.RESET}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.RESET}")
        print("=" * 50)

        return self.failed == 0

    def test_health(self) -> None:
        self.section("Health Check")
        try:
            health = self.client.health()
            if health.status == "ok":
                self.pass_test("GET /health returns ok")
            else:
                self.fail_test("GET /health", f"Unexpected status: {health.status}")
        except Exception as e:
            self.fail_test("GET /health", str(e))

    def test_authentication(self) -> None:
        self.section("Authentication")

        # Test without auth
        try:
            bad_client = R2IndexClient(index_api_url=self.api_url, index_api_token="")
            bad_client.list_files()
            bad_client.close()
            self.fail_test("Should reject empty token")
        except AuthenticationError:
            self.pass_test("Rejects empty token (401/403)")
        except Exception as e:
            self.fail_test("Auth test", str(e))

        # Test with wrong token
        try:
            bad_client = R2IndexClient(index_api_url=self.api_url, index_api_token="wrong-token")
            bad_client.list_files()
            bad_client.close()
            self.fail_test("Should reject invalid token")
        except AuthenticationError:
            self.pass_test("Rejects invalid token (403)")
        except Exception as e:
            self.fail_test("Auth test", str(e))

        # Test with valid token
        try:
            self.client.list_files()
            self.pass_test("Accepts valid token (200)")
        except Exception as e:
            self.fail_test("Valid token test", str(e))

    def test_file_crud(self) -> str | None:
        self.section("File CRUD Operations")
        file_id = None

        # Create file
        try:
            create_request = FileCreateRequest(
                bucket="e2e-test-bucket",
                category="e2e-test",
                entity="test-entity",
                extension="txt",
                media_type="text/plain",
                remote_path="/e2e-tests",
                remote_filename="test-file.txt",
                remote_version="v1",
                name="E2E Test File",
                size=1024,
                md5="d41d8cd98f00b204e9800998ecf8427e",
                sha1="da39a3ee5e6b4b0d3255bfef95601890afd80709",
                sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                sha512="cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
                tags=["e2e", "test"],
            )
            file_record = self.client.create_file(create_request)
            file_id = file_record.id
            self.pass_test(f"POST /files - Create file (id: {file_id})")
        except Exception as e:
            self.fail_test("POST /files - Create file", str(e))
            return None

        # Get file by ID
        try:
            file_record = self.client.get_file(file_id)
            if file_record.id == file_id:
                self.pass_test("GET /files/:id - Get file by ID")
            else:
                self.fail_test("GET /files/:id", "ID mismatch")
        except Exception as e:
            self.fail_test("GET /files/:id", str(e))

        # Update file
        try:
            update_request = FileUpdateRequest(
                name="Updated E2E Test File",
                size=2048,
            )
            updated = self.client.update_file(file_id, update_request)
            if updated.name == "Updated E2E Test File" and updated.size == 2048:
                self.pass_test("PUT /files/:id - Update file")
            else:
                self.fail_test("PUT /files/:id", "Update not applied")
        except Exception as e:
            self.fail_test("PUT /files/:id", str(e))

        # List files with filters
        try:
            response = self.client.list_files(category="e2e-test")
            self.pass_test("GET /files?category= - List with filter")
        except Exception as e:
            self.fail_test("GET /files?category=", str(e))

        # List files with bucket filter
        try:
            response = self.client.list_files(bucket="e2e-test-bucket")
            self.pass_test("GET /files?bucket= - List with bucket filter")
        except Exception as e:
            self.fail_test("GET /files?bucket=", str(e))

        # List files with tags filter
        try:
            response = self.client.list_files(tags=["e2e", "test"])
            self.pass_test("GET /files?tags= - List with tags filter")
        except Exception as e:
            self.fail_test("GET /files?tags=", str(e))

        # Get file by tuple
        try:
            remote_tuple = RemoteTuple(
                bucket="e2e-test-bucket",
                remote_path="/e2e-tests",
                remote_filename="test-file.txt",
                remote_version="v1",
            )
            file_record = self.client.get_file_by_tuple(remote_tuple)
            if file_record.id == file_id:
                self.pass_test("GET /files/by-tuple - Get by remote tuple")
            else:
                self.fail_test("GET /files/by-tuple", "ID mismatch")
        except Exception as e:
            self.fail_test("GET /files/by-tuple", str(e))

        # Get nested index
        try:
            index = self.client.get_index(category="e2e-test")
            self.pass_test("GET /files/index - Nested index")
        except Exception as e:
            self.fail_test("GET /files/index", str(e))

        return file_id

    def test_download_tracking(self) -> None:
        self.section("Download Tracking")

        # Record downloads
        try:
            from elaunira.r2index import DownloadRecordRequest

            for i in range(4):
                download_request = DownloadRecordRequest(
                    file_id=f"e2e-test-file-{i}",  # This won't link to real file but tests the endpoint
                    ip_address=f"192.168.1.{100 + i}",
                    user_agent="E2E-Test-Agent/1.0",
                )
                self.client.record_download(download_request)
            self.pass_test("POST /downloads - Record downloads")
        except Exception as e:
            self.fail_test("POST /downloads", str(e))

    def test_analytics(self) -> None:
        self.section("Analytics")

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=1)
        end = now + timedelta(hours=1)

        # Timeseries
        try:
            response = self.client.get_timeseries(start=start, end=end, granularity="hour")
            self.pass_test("GET /analytics/timeseries - Time series data")
        except Exception as e:
            self.fail_test("GET /analytics/timeseries", str(e))

        # Summary
        try:
            response = self.client.get_summary(start=start, end=end)
            self.pass_test("GET /analytics/summary - Summary stats")
        except Exception as e:
            self.fail_test("GET /analytics/summary", str(e))

        # By IP
        try:
            response = self.client.get_downloads_by_ip(
                ip_address="192.168.1.100",
                start=start,
                end=end,
            )
            self.pass_test("GET /analytics/by-ip - Downloads by IP")
        except Exception as e:
            self.fail_test("GET /analytics/by-ip", str(e))

        # User agents
        try:
            response = self.client.get_user_agents(start=start, end=end)
            self.pass_test("GET /analytics/user-agents - User agent stats")
        except Exception as e:
            self.fail_test("GET /analytics/user-agents", str(e))

    def test_file_deletion(self, file_id: str | None) -> None:
        self.section("File Deletion")

        # Delete by ID
        if file_id:
            try:
                self.client.delete_file(file_id)
                self.pass_test("DELETE /files/:id - Delete by ID")
            except Exception as e:
                self.fail_test("DELETE /files/:id", str(e))

        # Create and delete by tuple
        try:
            create_request = FileCreateRequest(
                bucket="e2e-test-bucket",
                category="e2e-test",
                entity="test-entity-2",
                extension="txt",
                media_type="text/plain",
                remote_path="/e2e-tests",
                remote_filename="test-file-2.txt",
                remote_version="v1",
                size=512,
                md5="d41d8cd98f00b204e9800998ecf8427e",
                sha1="da39a3ee5e6b4b0d3255bfef95601890afd80709",
                sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                sha512="cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
            )
            self.client.create_file(create_request)

            remote_tuple = RemoteTuple(
                bucket="e2e-test-bucket",
                remote_path="/e2e-tests",
                remote_filename="test-file-2.txt",
                remote_version="v1",
            )
            self.client.delete_file_by_tuple(remote_tuple)
            self.pass_test("DELETE /files - Delete by remote tuple")
        except Exception as e:
            self.fail_test("DELETE /files (tuple)", str(e))

    def test_error_handling(self) -> None:
        self.section("Error Handling")

        # 404 for non-existent file
        try:
            self.client.get_file("non-existent-id-12345")
            self.fail_test("Should raise NotFoundError")
        except NotFoundError:
            self.pass_test("GET /files/:id - Returns 404 for non-existent")
        except Exception as e:
            self.fail_test("404 test", str(e))

        # 400 for invalid input
        try:
            invalid_request = FileCreateRequest(
                bucket="",  # Invalid: empty bucket
                category="test",
                entity="test",
                extension="txt",
                media_type="text/plain",
                remote_path="/test",
                remote_filename="test.txt",
                remote_version="v1",
                size=100,
                md5="d41d8cd98f00b204e9800998ecf8427e",
                sha1="da39a3ee5e6b4b0d3255bfef95601890afd80709",
                sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                sha512="cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
            )
            self.client.create_file(invalid_request)
            self.fail_test("Should raise ValidationError")
        except ValidationError:
            self.pass_test("POST /files - Returns 400 for invalid input")
        except Exception as e:
            self.fail_test("400 test", str(e))

    def test_maintenance(self) -> None:
        self.section("Maintenance")

        try:
            response = self.client.cleanup_downloads()
            self.pass_test(f"POST /maintenance/cleanup-downloads (deleted: {response.deleted_count})")
        except Exception as e:
            self.fail_test("POST /maintenance/cleanup-downloads", str(e))


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python e2e_test.py <api_url> <api_token>")
        print("Example: python e2e_test.py https://r2index.example.com my-secret-token")
        sys.exit(1)

    api_url = sys.argv[1]
    api_token = sys.argv[2]

    test = E2ETest(api_url, api_token)
    success = test.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
