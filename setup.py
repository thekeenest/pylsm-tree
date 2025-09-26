from setuptools import setup, find_packages

setup(
    name="pylsm-tree",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "msgpack>=1.0.5",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
            "isort>=5.0.0",
            "plotly>=5.13.0",
            "numpy>=1.23.0",
            "tqdm>=4.65.0",
        ],
        "compression": [
            "python-snappy>=0.6.1",
            "zstandard>=0.21.0",
        ],
        "bench": [
            "python-rocksdb>=0.7.0",
        ],
    },
    author="thekeenest",
    author_email="your.email@example.com",
    description="High-performance LSM-Tree storage engine in Python",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/thekeenest/pylsm-tree",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
