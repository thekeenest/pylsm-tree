# 🌳 PyLSM: High-Performance LSM Storage Engine

[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/thekeenest/pylsm-tree/actions/workflows/tests.yml/badge.svg)](https://github.com/thekeenest/pylsm-tree/actions)

A high-performance Log-Structured Merge-Tree (LSM) storage engine implemented in pure Python with asyncio. Designed for educational purposes and research experimentation while maintaining production-grade architecture patterns.

## ✨ Features

- 🚀 Asynchronous I/O throughout the entire stack
- 📝 Write-Ahead Logging (WAL) for durability
- 🗄️ SkipList-based MemTable with configurable size
- 📦 SSTable format with Bloom filters
- 🔄 Multi-level compaction strategies
- 🗜️ Efficient data compression
- 📊 Built-in metrics and performance analytics
- 🧪 Comprehensive test suite

## 🚀 Quick Start

```bash
pip install pylsm-tree
```

```python
import asyncio
from pylsm import DB

async def main():
    # Open database
    db = DB("./data")
    await db.open()
    
    # Basic operations
    await db.set(b"key", b"value")
    value = await db.get(b"key")
    await db.delete(b"key")
    
    # Close properly
    await db.close()

asyncio.run(main())
```

## 🔧 Installation for Development

```bash
git clone https://github.com/thekeenest/pylsm-tree.git
cd pylsm-tree
pip install -e ".[dev]"
```

## 🧪 Running Tests

```bash
pytest tests/ -v --cov=pylsm
```

## 📊 Benchmarks

Run benchmarks against RocksDB:

```bash
python benchmarks/run_benchmarks.py --size 1000000 --value-size 1024
```

View interactive dashboard:

```bash
python -m pylsm.analytics
```

## 🏗️ Architecture

The engine is built around these core components:

1. **Write Path**
   - Write-Ahead Log (WAL) for durability
   - MemTable implementation using SkipList
   - SSTable generation with Bloom filters

2. **Read Path**
   - Bloom filter checking
   - Binary search in sparse index
   - Multi-level merge during compaction

3. **Background Operations**
   - Asynchronous compaction strategies
   - Metrics collection and monitoring

## 📈 Performance

Key performance characteristics:

- Write amplification: ~1.5x
- Read amplification: ~2x (with Bloom filters)
- Space amplification: ~1.2x

Detailed benchmarks available in the [benchmarks](./benchmarks) directory.

## 📚 Resources

- [Design Documentation](./docs/DESIGN.md)
- [Contributing Guide](./CONTRIBUTING.md)
- [API Reference](./docs/API.md)

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) first.

## 📄 License

This project is MIT licensed.
