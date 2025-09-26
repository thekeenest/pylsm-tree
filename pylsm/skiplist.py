"""A very small skip-list implementation to be used as the memtable.

The implementation is *not* optimised for production use—its goal is to keep
complexity low while supporting the subset of operations required by the LSM
engine (set/get/iter & size).

Complexities (average case):
    • search   – O(log n)
    • insert   – O(log n)
    • iterate  – O(n)

The probabilistic height algorithm uses the classic 50 % branching factor.
"""
from __future__ import annotations

from collections.abc import Iterator
from random import random
from typing import Generic, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")

_MAX_LEVEL = 16  # Supports > 65k elements on average.
_P = 0.5


def _random_level() -> int:
    lvl = 1
    while random() < _P and lvl < _MAX_LEVEL:
        lvl += 1
    return lvl


class _Node(Generic[K, V]):
    __slots__ = ("key", "value", "forward")

    def __init__(self, key: Optional[K], value: Optional[V], level: int):
        self.key = key
        self.value = value
        self.forward: list[Optional[_Node[K, V]]] = [None] * level

    def __repr__(self) -> str:  # pragma: no cover
        return f"Node<{self.key!r}:{self.value!r}>"  # type: ignore[arg-type]


class SkipList(Generic[K, V]):
    """Simple skip-list mapping sorted keys to arbitrary values."""

    def __init__(self):
        self._level = 1
        self._size = 0
        self._header: _Node[K, V] = _Node(None, None, _MAX_LEVEL)

    # ---------------------------------------------------------------------
    # Mutation API
    # ---------------------------------------------------------------------
    def set(self, key: K, value: V) -> None:
        """Insert or update `key` with `value`."""
        update: list[_Node[K, V]] = [self._header] * _MAX_LEVEL
        x = self._header
        for i in reversed(range(self._level)):
            while (nxt := x.forward[i]) and nxt.key < key:  # type: ignore[operator]
                x = nxt
            update[i] = x
        x = x.forward[0]
        if x and x.key == key:  # Update
            x.value = value
            return
        lvl = _random_level()
        if lvl > self._level:
            for i in range(self._level, lvl):
                update[i] = self._header
            self._level = lvl
        new_node: _Node[K, V] = _Node(key, value, lvl)
        for i in range(lvl):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
        self._size += 1

    # ---------------------------------------------------------------------
    # Query API
    # ---------------------------------------------------------------------
    def get(self, key: K) -> Optional[V]:
        x = self._header
        for i in reversed(range(self._level)):
            while (nxt := x.forward[i]) and nxt.key < key:  # type: ignore[operator]
                x = nxt
        x = x.forward[0]
        if x and x.key == key:
            return x.value
        return None

    def __len__(self) -> int:  # pragma: no cover
        return self._size

    # ------------------------------------------------------------------
    # Iteration helpers (ordered)
    # ------------------------------------------------------------------
    def __iter__(self) -> Iterator[tuple[K, V]]:  # pragma: no cover
        x = self._header.forward[0]
        while x is not None:
            yield x.key, x.value  # type: ignore[misc]
            x = x.forward[0]
