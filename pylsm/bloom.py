"""A *very* small Bloom filter implementation.

Main goals:
    • zero external dependencies
    • serialisable to bytes so it can be appended to an SSTable

Not optimised for production use.
"""
from __future__ import annotations

import math
import struct
from hashlib import blake2b
from typing import ClassVar

__all__ = ["BloomFilter"]


class BloomFilter:
    """Simple Bloom filter backed by a bit-array."""

    _FMT_HDR: ClassVar[str] = "!II"  # k, m

    def __init__(self, m: int, k: int):
        self.m = m  # bits
        self.k = k  # hash functions
        self._bits = bytearray((m + 7) // 8)

    # -------------------------------------------------------
    # Construction helpers 🏗️
    # -------------------------------------------------------
    @classmethod
    def from_capacity(cls, n: int, fp: float = 0.01) -> "BloomFilter":
        """Create sized BF that can store `n` items with ≤ `fp` false-positive rate."""
        m = -n * math.log(fp) / (math.log(2) ** 2)
        k = (m / n) * math.log(2)
        return cls(int(m), max(1, int(k)))

    # -------------------------------------------------------
    # Hash helpers
    # -------------------------------------------------------
    def _hashes(self, item: bytes):
        h = blake2b(item, digest_size=16).digest()
        a, b = struct.unpack("!QQ", h)
        for i in range(self.k):
            yield (a + i * b) % self.m

    # -------------------------------------------------------
    # API
    # -------------------------------------------------------
    def add(self, item: bytes):
        for pos in self._hashes(item):
            self._bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, item: bytes) -> bool:
        return all(self._bits[pos // 8] & (1 << (pos % 8)) for pos in self._hashes(item))

    # -------------------------------------------------------
    # Serialisation 📦
    # -------------------------------------------------------
    def to_bytes(self) -> bytes:
        return struct.pack(self._FMT_HDR, self.k, self.m) + bytes(self._bits)

    @classmethod
    def from_bytes(cls, blob: bytes) -> "BloomFilter":
        k, m = struct.unpack(cls._FMT_HDR, blob[:8])
        bf = cls(m, k)
        bf._bits[:] = blob[8:]
        return bf
