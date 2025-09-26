"""High-level DB interface tying all LSM components together.

Currently supports a *single* level of SSTables and no background
compaction yet. The goal is to demonstrate the end-to-end write & read path
before adding more sophisticated features.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Dict, List, Iterator
import contextlib

_COMPACTION_INTERVAL = 0.5  # seconds background loop wakeup

from .skiplist import SkipList
from .sstable import SSTable, Record
from .wal import WAL

__all__ = ["DB"]

_MEMTABLE_LIMIT = 512 * 1024  # 512 KiB for faster tests
_COMPACTION_TRIGGER = 4  # when #sstables > trigger, run compaction


class DB:
    """Tiny LSM DB backed by a directory on disk."""

    def __init__(self, datadir: str | Path):
        self._dir = Path(datadir)
        self._mem = SkipList[bytes, bytes | None]()
        self._wal = WAL(self._dir / "wal")
        self._sstables: list[SSTable] = []  # newest first (level-0 like)
        self._lock = asyncio.Lock()
        self._background: Optional[asyncio.Task[None]] = None

    # ------------------------------------------------------------------
    # Lifecycle 🔧
    # ------------------------------------------------------------------
    async def open(self):
        """Initialise engine, replay WALs, load SSTables and spawn background tasks."""
        self._dir.mkdir(parents=True, exist_ok=True)
        # 1. Load SSTable metadata (newest first)
        for p in sorted(self._dir.glob("sst_*.sst"), reverse=True):
            sst = SSTable(p)
            await sst.open()
            self._sstables.append(sst)
        # 2. Recover WAL segments
        wal_dir = self._dir / "wal"
        if wal_dir.exists():
            segs = sorted(wal_dir.glob("wal_*.log"), key=WAL._parse_seq)
            for seg in segs:
                for k, v, tomb in await WAL.replay(seg):
                    self._mem.set(k, None if tomb else v)
        # 3. open fresh WAL for future writes (seq = last+1)
        next_seq = (max((WAL._parse_seq(p) for p in wal_dir.glob("wal_*.log")), default=-1) + 1) if wal_dir.exists() else 0
        self._wal = WAL(wal_dir, seq=next_seq)
        await self._wal.open()
        # 4. Spawn compaction task
        self._background = asyncio.create_task(self._compaction_loop())

    async def close(self):
        # stop background task
        if self._background:
            self._background.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._background
        await self._wal.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def set(self, key: bytes, value: bytes):
        async with self._lock:
            await self._wal.append(key, value)
            self._mem.set(key, value)
            if self._mem_size() >= _MEMTABLE_LIMIT:
                await self._flush()

    async def delete(self, key: bytes):
        async with self._lock:
            await self._wal.append(key, None, tombstone=True)
            self._mem.set(key, None)
            if self._mem_size() >= _MEMTABLE_LIMIT:
                await self._flush()

    async def get(self, key: bytes) -> Optional[bytes]:
        # Fast in-mem path
        if (val := self._mem.get(key)) is not None:
            return None if val is None else val
        # Search SSTables newest→oldest
        for sst in self._sstables:
            if (val := await sst.get(key)) is not None:
                return val
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _mem_size(self) -> int:
        return sum(len(k) + (len(v) if v else 0) for k, v in self._mem)

    async def _flush(self):
        """Flush current memtable to disk creating new SSTable."""
        # Freeze old mem
        items = sorted((k, v, v is None) for k, v in self._mem)
        self._mem = SkipList()
        # Rotate WAL
        await self._wal.close()
        self._wal = WAL(self._dir / "wal", seq=len(self._sstables) + 1)
        await self._wal.open()
        # Write SSTable
        path = self._dir / f"sst_{len(self._sstables):06d}.sst"
        sst = await SSTable.create(path, items)
        self._sstables.insert(0, sst)
        # maybe trigger compaction
        if len(self._sstables) > _COMPACTION_TRIGGER:
            await self._compact_level0()
