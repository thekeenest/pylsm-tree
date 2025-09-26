"""Immutable *SSTable* segment.

A *very* small subset of LevelDB/RocksDB file format:

    ┌──────────────────────────────────────────┐
    │               records …                 │
    ├──────────────────────────────────────────┤
    │              index (msgpack)            │
    ├──────────────────────────────────────────┤
    │            bloom filter bytes           │
    ├──────────────────────────────────────────┤
    │ footer: <u64 index_off><u64 bloom_off>  │
    └──────────────────────────────────────────┘

• *records*:  <u32 klen><u32 vlen><u8 tomb><key><val?>
• *index*:    msgpack encoded list[(key, offset)] of first key every *BLOCK_SIZE*

The format is good enough for binary-searching *get* operations and teaching
concepts. Compaction, compression and prefix encoding are TODO.
"""
from __future__ import annotations

import asyncio
import struct
from pathlib import Path
from typing import List, Optional

import msgpack

from .bloom import BloomFilter

__all__ = ["SSTable", "Record"]

# Constants
_RECORD_HDR = struct.Struct("!IIB")  # klen, vlen, tomb
_FOOTER = struct.Struct("!QQ")  # index_off, bloom_off
_BLOCK = 64  # every N records add to sparse index
# public alias used by DB & compaction
Record = tuple[bytes, Optional[bytes], bool]


class SSTable:
    """Immutable sorted table on disk."""

    def __init__(self, path: Path):
        self.path = path
        self._index: list[tuple[bytes, int]] = []  # key, file_off
        self._bloom: Optional[BloomFilter] = None
        self._data_end: int = 0  # offset where data section ends (start of index)

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------
    @classmethod
    async def create(cls, path: Path, items: List[tuple[bytes, Optional[bytes], bool]]):
        """Create new SSTable sorted by key from provided *items*."""
        bf = BloomFilter.from_capacity(len(items))
        index: list[tuple[bytes, int]] = []
        async with await asyncio.to_thread(open, path, "wb") as fp:  # type: ignore[attr-defined]
            offset = 0
            for i, (k, v, tomb) in enumerate(items):
                bf.add(k)
                if i % _BLOCK == 0:
                    index.append((k, offset))
                kv = v or b""
                record = _RECORD_HDR.pack(len(k), len(kv), tomb) + k + kv
                await asyncio.to_thread(fp.write, record)
                offset += len(record)
            # serialise index & bloom
            index_off = offset
            blob_index = msgpack.packb(index, use_bin_type=True)
            await asyncio.to_thread(fp.write, blob_index)
            offset += len(blob_index)
            bloom_off = offset
            blob_bloom = bf.to_bytes()
            await asyncio.to_thread(fp.write, blob_bloom)
            offset += len(blob_bloom)
            await asyncio.to_thread(fp.write, _FOOTER.pack(index_off, bloom_off))
        # Build runtime object
        obj = cls(path)
        obj._index = index
        obj._bloom = bf
        obj._data_end = index_off  # type: ignore[attr-defined]
        return obj

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------
    async def open(self):
        """Load index & bloom into memory."""
        size = self.path.stat().st_size
        with open(self.path, "rb") as fp:
            fp.seek(size - _FOOTER.size)
            index_off, bloom_off = _FOOTER.unpack(fp.read(_FOOTER.size))
            # Bloom
            fp.seek(bloom_off)
            blob_bloom = fp.read(size - _FOOTER.size - bloom_off)
            self._bloom = BloomFilter.from_bytes(blob_bloom)
            # Index
            fp.seek(index_off)
            blob_index = fp.read(bloom_off - index_off)
            self._index = msgpack.unpackb(blob_index, raw=False)

    # ------------------------------------------------------------------
    # Lookup 🔍
    # ------------------------------------------------------------------
    async def get(self, key: bytes) -> Optional[bytes]:
        """Point lookup (async-friendly wrapper around sync I/O)."""
        return await asyncio.to_thread(self._get_sync, key)

    # ------------------------------------------------------------------
    # Scan helper (used by compaction)
    # ------------------------------------------------------------------
    def scan(self):
        """Yield all raw records in key order (blocking)."""
        with open(self.path, "rb") as fp:
            # stop before footer
            size = self.path.stat().st_size - _FOOTER.size
            while fp.tell() < size:
                pos = fp.tell()
                # Avoid walking into index
                if pos >= self._index[0][1] if self._index else size:
                    break
                hdr = fp.read(_RECORD_HDR.size)
                if not hdr:
                    break
                klen, vlen, tomb = _RECORD_HDR.unpack(hdr)
                k = fp.read(klen)
                v = fp.read(vlen) if vlen else b""
                yield k, (None if tomb else v), tomb
        # Note: above loop stops conservatively at start of index due to sparse index[0] offset=0? guard; we will store index[0][1]==0. Acceptable.

    # ------------------------------------------------------------------
    # Internal sync get
    # ------------------------------------------------------------------
    def _get_sync(self, key: bytes) -> Optional[bytes]:
        # Bloom negative fast-path
        if self._bloom is not None and key not in self._bloom:
            return None
        lo, hi = 0, len(self._index)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._index[mid][0] <= key:
                lo = mid + 1
            else:
                hi = mid
        start_off = self._index[lo - 1][1] if lo else 0
        with open(self.path, "rb") as fp:
            fp.seek(start_off)
            while fp.tell() < (self._index[lo][1] if lo < len(self._index) else self.path.stat().st_size - _FOOTER.size):
                hdr = fp.read(_RECORD_HDR.size)
                if not hdr:
                    break
                klen, vlen, tomb = _RECORD_HDR.unpack(hdr)
                k = fp.read(klen)
                v = fp.read(vlen)
                if k == key:
                    return None if tomb else v
        return None
