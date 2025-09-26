"""LSM-tree compaction strategies implementation.

This module implements both leveled and tiered compaction strategies:

Leveled Compaction:
- Data is organized in L0, L1, L2... levels
- Size ratio between levels (usually 10x)
- L0 files may overlap, other levels are sorted
- Better read performance, higher write amplification

Tiered Compaction:
- Files within same size range are grouped
- When enough files accumulate, merged into next tier
- Higher read amplification, better write performance
"""
from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Iterator, AsyncIterator, Dict

from .sstable import SSTable, Record

logger = logging.getLogger(__name__)

@dataclass
class Level:
    """Represents a level in the LSM tree."""
    number: int
    tables: List[SSTable]
    size_limit: int  # Maximum total size in bytes

    @property
    def size(self) -> int:
        """Total size of all SSTables in this level."""
        return sum(t.path.stat().st_size for t in self.tables)

class CompactionStrategy:
    """Base class for compaction strategies."""
    
    def __init__(self, base_level_size: int = 64 * 1024 * 1024):  # 64MB
        self.base_level_size = base_level_size
        self.size_ratio = 10  # Size multiplier between levels
        self.levels: List[Level] = []

    def _create_level(self, number: int) -> Level:
        """Create a new level with appropriate size limit."""
        size_limit = self.base_level_size * (self.size_ratio ** number)
        return Level(number, [], size_limit)

    async def add_table(self, table: SSTable) -> List[SSTable]:
        """Add a new SSTable and return tables that should be compacted."""
        raise NotImplementedError

    async def compact(self, tables: List[SSTable], level: int) -> SSTable:
        """Merge multiple SSTables into one."""
        raise NotImplementedError

class LeveledCompaction(CompactionStrategy):
    """Leveled compaction strategy (similar to RocksDB)."""

    async def add_table(self, table: SSTable) -> List[SSTable]:
        """Add new SSTable to L0, trigger compaction if needed."""
        # Ensure we have at least one level
        if not self.levels:
            self.levels.append(self._create_level(0))

        # Add to L0
        self.levels[0].tables.append(table)

        # Check if L0 needs compaction
        if len(self.levels[0].tables) > 4:  # L0 file count trigger
            return self.levels[0].tables
        return []

    async def compact(self, tables: List[SSTable], level: int) -> SSTable:
        """Merge overlapping SSTables into the next level."""
        # Ensure target level exists
        while len(self.levels) <= level + 1:
            self.levels.append(self._create_level(len(self.levels)))

        # Create merge iterator
        iterators = [table.scan() for table in tables]
        merged = self._merge_iterators(iterators)

        # Write new SSTable
        target_level = self.levels[level + 1]
        new_path = Path(tables[0].path.parent) / f"L{level+1}_merged_{len(target_level.tables)}.sst"
        return await SSTable.create(new_path, list(merged))

    def _merge_iterators(self, iterators: List[Iterator[Record]]) -> Iterator[Record]:
        """Merge multiple sorted SSTable iterators."""
        # Initialize heap with first item from each iterator
        heap = []
        for it in iterators:
            try:
                item = next(it)
                heapq.heappush(heap, (item[0], item, it))
            except StopIteration:
                pass

        # Merge while maintaining order
        last_key = None
        while heap:
            key, (k, v, tomb), it = heapq.heappop(heap)
            
            # Skip duplicates, keep most recent (they're in order)
            if key != last_key:
                yield (k, v, tomb)
                last_key = key

            # Get next item from this iterator
            try:
                item = next(it)
                heapq.heappush(heap, (item[0], item, it))
            except StopIteration:
                pass

class TieredCompaction(CompactionStrategy):
    """Tiered compaction strategy (similar to Cassandra)."""

    async def add_table(self, table: SSTable) -> List[SSTable]:
        """Add table to smallest tier, merge if size threshold reached."""
        # Ensure we have at least one level
        if not self.levels:
            self.levels.append(self._create_level(0))

        level = self.levels[0]
        level.tables.append(table)

        # Check if current level should be compacted
        if level.size >= level.size_limit:
            return level.tables
        return []

    async def compact(self, tables: List[SSTable], level: int) -> SSTable:
        """Merge all tables in a tier into a single larger table."""
        # Similar to leveled compaction but simpler - we just merge all
        iterators = [table.scan() for table in tables]
        merged = self._merge_iterators(iterators)

        # Write new SSTable in next tier
        new_path = Path(tables[0].path.parent) / f"T{level+1}_merged_{len(tables)}.sst"
        return await SSTable.create(new_path, list(merged))
