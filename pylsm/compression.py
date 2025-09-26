"""Storage compression utilities.

Supports multiple compression algorithms:
- Snappy (fast compression/decompression)
- Zstd (better compression ratio)
- None (no compression)

Each block in SSTable can be compressed independently.
"""
from __future__ import annotations

import enum
from typing import Optional, Union

try:
    import snappy
    HAS_SNAPPY = True
except ImportError:
    HAS_SNAPPY = False

try:
    import zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False


class CompressionType(enum.Enum):
    """Available compression algorithms."""
    NONE = 0
    SNAPPY = 1
    ZSTD = 2


class Compression:
    """Compression utility class."""

    def __init__(self, algorithm: Union[str, CompressionType] = "snappy", level: int = 3):
        if isinstance(algorithm, str):
            algorithm = CompressionType[algorithm.upper()]
        
        self.algorithm = algorithm
        self.level = level

        if algorithm == CompressionType.SNAPPY and not HAS_SNAPPY:
            raise ImportError("Snappy compression requested but python-snappy not installed")
        if algorithm == CompressionType.ZSTD and not HAS_ZSTD:
            raise ImportError("Zstd compression requested but python-zstd not installed")

    def compress(self, data: bytes) -> bytes:
        """Compress a block of data."""
        if self.algorithm == CompressionType.NONE:
            return data
        elif self.algorithm == CompressionType.SNAPPY:
            return snappy.compress(data)
        elif self.algorithm == CompressionType.ZSTD:
            return zstd.compress(data, self.level)
        raise ValueError(f"Unknown compression algorithm: {self.algorithm}")

    def decompress(self, data: bytes) -> bytes:
        """Decompress a block of data."""
        if self.algorithm == CompressionType.NONE:
            return data
        elif self.algorithm == CompressionType.SNAPPY:
            return snappy.decompress(data)
        elif self.algorithm == CompressionType.ZSTD:
            return zstd.decompress(data)
        raise ValueError(f"Unknown compression algorithm: {self.algorithm}")


class PrefixCompression:
    """Prefix compression for sorted keys."""

    @staticmethod
    def compress_keys(keys: list[bytes]) -> tuple[list[bytes], list[int]]:
        """Compress a sorted list of keys using prefix compression.
        
        Returns:
            tuple: (compressed_keys, prefix_lengths)
        """
        if not keys:
            return [], []

        result = []
        prefix_lengths = []
        last_key = keys[0]
        result.append(last_key)
        prefix_lengths.append(0)

        for key in keys[1:]:
            # Find common prefix length
            prefix_len = 0
            for i in range(min(len(last_key), len(key))):
                if last_key[i] != key[i]:
                    break
                prefix_len += 1

            # Store only the suffix
            result.append(key[prefix_len:])
            prefix_lengths.append(prefix_len)
            last_key = key

        return result, prefix_lengths

    @staticmethod
    def decompress_keys(compressed: list[bytes], prefix_lengths: list[int]) -> list[bytes]:
        """Decompress keys compressed with prefix compression."""
        if not compressed:
            return []

        result = []
        last_key = compressed[0]
        result.append(last_key)

        for key_suffix, prefix_len in zip(compressed[1:], prefix_lengths[1:]):
            key = last_key[:prefix_len] + key_suffix
            result.append(key)
            last_key = key

        return result
