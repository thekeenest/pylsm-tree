"""Unit tests for the SSTable implementation."""
import os
import tempfile
from pathlib import Path

import pytest

from pylsm.sstable import SSTable


@pytest.fixture
async def sample_data():
    """Generate sample data for SSTable tests."""
    return [
        (b"key1", b"value1", False),
        (b"key2", b"value2", False),
        (b"key3", None, True),  # Tombstone
        (b"key4", b"value4", False),
    ]


@pytest.fixture
async def sstable(sample_data):
    """Create a temporary SSTable for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.sst"
        table = await SSTable.create(path, sample_data)
        yield table


async def test_creation(sstable, sample_data):
    """Test SSTable creation and basic properties."""
    assert sstable.path.exists()
    assert sstable.path.stat().st_size > 0


async def test_get_existing(sstable):
    """Test retrieval of existing keys."""
    assert await sstable.get(b"key1") == b"value1"
    assert await sstable.get(b"key2") == b"value2"
    assert await sstable.get(b"key4") == b"value4"


async def test_get_tombstone(sstable):
    """Test retrieval of tombstoned keys."""
    assert await sstable.get(b"key3") is None


async def test_get_nonexistent(sstable):
    """Test retrieval of non-existent keys."""
    assert await sstable.get(b"nonexistent") is None


async def test_scan(sstable, sample_data):
    """Test scanning all records."""
    records = list(sstable.scan())
    assert len(records) == len(sample_data)
    for (k1, v1, t1), (k2, v2, t2) in zip(records, sample_data):
        assert k1 == k2
        assert v1 == v2
        assert t1 == t2


async def test_large_keys(tmp_path):
    """Test handling of large keys."""
    large_key = os.urandom(1024)
    large_value = os.urandom(1024)
    data = [(large_key, large_value, False)]
    
    path = tmp_path / "large.sst"
    table = await SSTable.create(path, data)
    assert await table.get(large_key) == large_value


async def test_many_records(tmp_path):
    """Test handling of many records."""
    num_records = 10000
    data = [
        (f"key{i}".encode(), f"value{i}".encode(), False)
        for i in range(num_records)
    ]
    
    path = tmp_path / "many.sst"
    table = await SSTable.create(path, data)
    
    # Test random access
    for i in range(0, num_records, 100):
        key = f"key{i}".encode()
        expected = f"value{i}".encode()
        assert await table.get(key) == expected
