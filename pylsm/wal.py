"""Write-Ahead Log (WAL) implementation.

The WAL guarantees *durability* of updates by persisting them to disk before
acknowledging the client. It stores a sequence of (key, value, tombstone)
entries encoded with *msgpack* for compactness.

The API is intentionally minimal; the log file is append-only and only closed
when the corresponding memtable is flushed to an SSTable; a new WAL file is
then created.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Optional

import msgpack

__all__ = ["WAL", "Record"]

Record = tuple[bytes, Optional[bytes], bool]  # key, value, tombstone


class WAL:
    """Async write-ahead log.

    Parameters
    ----------
    dirpath: Path
        Directory where WAL segment files are created.
    loop: asyncio.AbstractEventLoop | None
        Custom event loop for unit testing.
    """

    _FILE_TEMPLATE = "wal_{seq:06d}.log"

    # ---------------------------------------------------------------
    # Helper utils
    # ---------------------------------------------------------------
    @staticmethod
    def _parse_seq(path: Path) -> int:
        """Extract numeric sequence from a wal_XXXXXX.log path."""
        return int(path.stem.split("_")[1])

    def __init__(self, dirpath: Path, *, seq: int = 0, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._dir = dirpath
        self._seq = seq
        self._loop = loop or asyncio.get_event_loop()
        self._fp: Optional[asyncio.StreamWriter] = None

    # ------------------------------------------------------------------
    # Lifecycle 💾
    # ------------------------------------------------------------------
    async def open(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / self._FILE_TEMPLATE.format(seq=self._seq)
        fp = await asyncio.to_thread(open, path, "ab", buffering=0)
        # Wrap into StreamWriter for uniform API.
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await self._loop.connect_write_pipe(lambda: protocol, fp)
        self._fp = asyncio.StreamWriter(transport, protocol, reader, self._loop)

    async def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            await self._fp.wait_closed()
            self._fp = None

    # ------------------------------------------------------------------
    # Append API ✏️
    # ------------------------------------------------------------------
    async def append(self, key: bytes, value: Optional[bytes], tombstone: bool = False) -> None:
        assert self._fp, "WAL not opened"
        rec = msgpack.packb((key, value, tombstone), use_bin_type=True)
        # Prepend length for recovery scanning.
        buff = len(rec).to_bytes(4, "big") + rec
        self._fp.write(buff)
        await self._fp.drain()

    # ------------------------------------------------------------------
    # Iteration 🔎 (for recovery)
    # ------------------------------------------------------------------
    @classmethod
    async def replay(cls, path: Path) -> list[Record]:
        """Replay WAL segment into memory (sync helper)."""
        records: list[Record] = []
        with open(path, "rb") as fp:
            while True:
                nbytes = fp.read(4)
                if not nbytes:
                    break
                (length,) = int.from_bytes(nbytes, "big"),
                blob = fp.read(length)
                key, value, tombstone = msgpack.unpackb(blob, raw=False)
                records.append((key, value, tombstone))
        return records
