"""Integration tests for the LSM DB."""
import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from pylsm import DB


@pytest.fixture
async def db():
    """Create a temporary DB instance for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db = DB(Path(tmpdir))
        await db.open()
        yield db
        await db.close()


async def test_basic_operations(db):
    """Test basic set/get/delete operations."""
    # Set and get
    await db.set(b"key1", b"value1")
    assert await db.get(b"key1") == b"value1"

    # Update
    await db.set(b"key1", b"value2")
    assert await db.get(b"key1") == b"value2"

    # Delete
    await db.delete(b"key1")
    assert await db.get(b"key1") is None


async def test_multiple_sstables(db):
    """Test that data is correctly distributed across multiple SSTables."""
    # Write enough data to trigger multiple SSTable creation
    for i in range(1000):
        key = f"key{i}".encode()
        value = f"value{i}".encode()
        await db.set(key, value)

    # Verify all data is readable
    for i in range(1000):
        key = f"key{i}".encode()
        expected = f"value{i}".encode()
        assert await db.get(key) == expected


async def test_concurrent_operations(db):
    """Test concurrent reads and writes."""
    async def writer():
        for i in range(100):
            key = f"concurrent_key{i}".encode()
            value = f"value{i}".encode()
            await db.set(key, value)

    async def reader():
        for i in range(100):
            key = f"concurrent_key{i}".encode()
            expected = f"value{i}".encode()
            value = await db.get(key)
            if value is not None:  # Key might not be written yet
                assert value == expected

    # Run concurrent operations
    writers = [writer() for _ in range(5)]
    readers = [reader() for _ in range(5)]
    await asyncio.gather(*writers, *readers)


async def test_recovery(db):
    """Test that the DB recovers correctly after crash."""
    # Write some data
    test_data = {
        b"key1": b"value1",
        b"key2": b"value2",
        b"key3": b"value3",
    }
    for k, v in test_data.items():
        await db.set(k, v)

    # Force close without cleanup
    db._background.cancel()  # type: ignore
    await db._wal.close()

    # Reopen and verify
    await db.open()
    for k, v in test_data.items():
        assert await db.get(k) == v


async def test_large_values(db):
    """Test handling of large values."""
    large_value = os.urandom(1024 * 1024)  # 1MB
    await db.set(b"large_key", large_value)
    assert await db.get(b"large_key") == large_value


async def test_empty_value(db):
    """Test handling of empty values."""
    await db.set(b"empty_key", b"")
    assert await db.get(b"empty_key") == b""


async def test_missing_key(db):
    """Test behavior for non-existent keys."""
    assert await db.get(b"nonexistent") is None
