#!/usr/bin/env python3
"""Benchmark suite for PyLSM comparing against RocksDB."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import plotly.graph_objects as go
try:
    import rocksdb
    HAS_ROCKSDB = True
except ImportError:
    HAS_ROCKSDB = False
    print("RocksDB not available, skipping RocksDB benchmarks")
from tqdm import tqdm

from pylsm import DB

class Metrics:
    def __init__(self):
        self.write_latencies: List[float] = []
        self.read_latencies: List[float] = []
        self.size_on_disk: List[Tuple[int, int]] = []  # (num_entries, size)
        self.compaction_times: List[float] = []

    def to_dict(self) -> Dict:
        return {
            "write_latencies": {
                "p50": np.percentile(self.write_latencies, 50),
                "p95": np.percentile(self.write_latencies, 95),
                "p99": np.percentile(self.write_latencies, 99),
            },
            "read_latencies": {
                "p50": np.percentile(self.read_latencies, 50),
                "p95": np.percentile(self.read_latencies, 95),
                "p99": np.percentile(self.read_latencies, 99),
            },
            "size_amplification": self._calculate_size_amplification(),
            "compaction_avg_time": np.mean(self.compaction_times),
        }

    def _calculate_size_amplification(self) -> float:
        if not self.size_on_disk:
            return 0.0
        last_entries, last_size = self.size_on_disk[-1]
        return last_size / (last_entries * self.size_on_disk[0][1])

    def plot_latencies(self, title: str, output_path: Path):
        fig = go.Figure()
        
        # Write latencies
        fig.add_trace(go.Box(
            y=self.write_latencies,
            name="Write Latency",
            boxpoints="outliers"
        ))
        
        # Read latencies
        fig.add_trace(go.Box(
            y=self.read_latencies,
            name="Read Latency",
            boxpoints="outliers"
        ))
        
        fig.update_layout(
            title=title,
            yaxis_title="Latency (ms)",
            boxmode="group"
        )
        
        fig.write_html(output_path)

class BenchmarkSuite:
    def __init__(self, db_path: Path, num_entries: int, value_size: int):
        self.db_path = db_path
        self.num_entries = num_entries
        self.value_size = value_size
        self.metrics = Metrics()
        self._keys = [f"key_{i}".encode() for i in range(num_entries)]
        self._values = [os.urandom(value_size) for _ in range(num_entries)]

    async def run_pylsm_benchmark(self):
        db_path = self.db_path / "pylsm"
        db = DB(db_path)
        await db.open()

        # Write benchmark
        for i in tqdm(range(self.num_entries), desc="PyLSM Write"):
            start = time.perf_counter()
            await db.set(self._keys[i], self._values[i])
            self.metrics.write_latencies.append((time.perf_counter() - start) * 1000)

        # Read benchmark
        for i in tqdm(range(self.num_entries), desc="PyLSM Read"):
            start = time.perf_counter()
            await db.get(self._keys[i])
            self.metrics.read_latencies.append((time.perf_counter() - start) * 1000)

        await db.close()
        
        # Size metrics
        total_size = sum(f.stat().st_size for f in db_path.glob("**/*") if f.is_file())
        self.metrics.size_on_disk.append((self.num_entries, total_size))

    def run_rocksdb_benchmark(self):
        db_path = self.db_path / "rocksdb"
        opts = rocksdb.Options()
        opts.create_if_missing = True
        db = rocksdb.DB(str(db_path), opts)

        metrics = Metrics()

        # Write benchmark
        for i in tqdm(range(self.num_entries), desc="RocksDB Write"):
            start = time.perf_counter()
            db.put(self._keys[i], self._values[i])
            metrics.write_latencies.append((time.perf_counter() - start) * 1000)

        # Read benchmark
        for i in tqdm(range(self.num_entries), desc="RocksDB Read"):
            start = time.perf_counter()
            db.get(self._keys[i])
            metrics.read_latencies.append((time.perf_counter() - start) * 1000)

        # Size metrics
        total_size = sum(f.stat().st_size for f in db_path.glob("**/*") if f.is_file())
        metrics.size_on_disk.append((self.num_entries, total_size))

        return metrics

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100000, help="Number of entries")
    parser.add_argument("--value-size", type=int, default=1024, help="Size of values in bytes")
    parser.add_argument("--output", type=Path, default=Path("benchmark_results"), help="Output directory")
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    
    # Run benchmarks
    suite = BenchmarkSuite(args.output, args.size, args.value_size)
    
    # PyLSM
    await suite.run_pylsm_benchmark()
    pylsm_metrics = suite.metrics
    
    # RocksDB (if available)
    rocksdb_metrics = suite.run_rocksdb_benchmark() if HAS_ROCKSDB else None
    
    # Generate reports
    pylsm_metrics.plot_latencies(
        "PyLSM Latency Distribution",
        args.output / "pylsm_latencies.html"
    )
    
    # Save metrics
    with open(args.output / "metrics.json", "w") as f:
        json.dump({
            "pylsm": pylsm_metrics.to_dict(),
            "rocksdb": rocksdb_metrics.to_dict(),
        }, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())
