"""Streaming checksum computation for large files."""

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .storage import _format_bytes

logger = logging.getLogger(__name__)

# 8MB chunk size for memory-efficient processing of large files
CHUNK_SIZE = 8 * 1024 * 1024


@dataclass
class ChecksumResult:
    """Result of checksum computation."""

    md5: str
    sha1: str
    sha256: str
    sha512: str
    size: int


def compute_checksums(file_path: str | Path) -> ChecksumResult:
    """
    Compute MD5, SHA1, SHA256, and SHA512 checksums for a file.

    Reads the file in chunks for memory-efficient processing of large files.
    All checksums are computed in a single pass through the file.

    Args:
        file_path: Path to the file to compute checksums for.

    Returns:
        ChecksumResult containing all checksums and file size.
    """
    file_path = Path(file_path)
    total_size = file_path.stat().st_size

    md5_hash = hashlib.md5()
    sha1_hash = hashlib.sha1()
    sha256_hash = hashlib.sha256()
    sha512_hash = hashlib.sha512()

    logger.info("Computing checksums for %s (%s)", file_path.name, _format_bytes(total_size))

    size = 0

    with open(file_path, "rb") as f:
        size = _compute_from_file_object(
            f, md5_hash, sha1_hash, sha256_hash, sha512_hash, total_size=total_size,
        )

    logger.info("Checksums computed for %s", file_path.name)

    return ChecksumResult(
        md5=md5_hash.hexdigest(),
        sha1=sha1_hash.hexdigest(),
        sha256=sha256_hash.hexdigest(),
        sha512=sha512_hash.hexdigest(),
        size=size,
    )


def compute_checksums_from_file_object(file_obj: BinaryIO) -> ChecksumResult:
    """
    Compute checksums from a file-like object.

    Args:
        file_obj: Binary file-like object to read from.

    Returns:
        ChecksumResult containing all checksums and total bytes read.
    """
    md5_hash = hashlib.md5()
    sha1_hash = hashlib.sha1()
    sha256_hash = hashlib.sha256()
    sha512_hash = hashlib.sha512()

    size = _compute_from_file_object(file_obj, md5_hash, sha1_hash, sha256_hash, sha512_hash)

    return ChecksumResult(
        md5=md5_hash.hexdigest(),
        sha1=sha1_hash.hexdigest(),
        sha256=sha256_hash.hexdigest(),
        sha512=sha512_hash.hexdigest(),
        size=size,
    )


def _compute_from_file_object(
    file_obj: BinaryIO,
    md5_hash: "hashlib._Hash",
    sha1_hash: "hashlib._Hash",
    sha256_hash: "hashlib._Hash",
    sha512_hash: "hashlib._Hash",
    total_size: int | None = None,
) -> int:
    """
    Internal helper to compute checksums from a file object.

    Returns the total number of bytes read.
    """
    size = 0
    last_log = time.monotonic()
    start = time.monotonic()

    while True:
        chunk = file_obj.read(CHUNK_SIZE)
        if not chunk:
            break

        size += len(chunk)
        md5_hash.update(chunk)
        sha1_hash.update(chunk)
        sha256_hash.update(chunk)
        sha512_hash.update(chunk)

        now = time.monotonic()
        if total_size and now - last_log >= 10.0:
            pct = size / total_size * 100
            elapsed = now - start
            speed = size / elapsed if elapsed > 0 else 0
            logger.info(
                "Checksumming: %s / %s (%.1f%%) — %s/s",
                _format_bytes(size), _format_bytes(total_size), pct, _format_bytes(speed),
            )
            last_log = now

    return size


async def compute_checksums_async(file_path: str | Path) -> ChecksumResult:
    """
    Compute checksums asynchronously.

    Note: This uses synchronous file I/O in a way that doesn't block the event loop
    for too long by processing in chunks. For truly async file I/O, consider using
    aiofiles, but for CPU-bound hashing, the benefit is minimal.

    Args:
        file_path: Path to the file to compute checksums for.

    Returns:
        ChecksumResult containing all checksums and file size.
    """
    import asyncio

    return await asyncio.to_thread(compute_checksums, file_path)
