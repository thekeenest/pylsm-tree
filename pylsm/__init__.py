"""PyLSM: A minimal yet extensible Log-Structured Merge-Tree storage engine in Python.

This package exposes a high-level DB API via `pylsm.DB` while keeping the
core primitives (memtable, WAL, SSTable, compaction) pluggable so that the
project can evolve for educational and research purposes.
"""

from __future__ import annotations

__all__ = [
    "DB",
]

from .db import DB
