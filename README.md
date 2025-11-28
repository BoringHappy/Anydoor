# Anydoor

A comprehensive Python toolkit for data engineering, quantitative analysis, and LLM applications.

[![CI](https://github.com/veinkr/anydoor/workflows/CI/badge.svg)](https://github.com/veinkr/anydoor/actions/workflows/ci.yml)
[![Test Coverage](https://codecov.io/gh/veinkr/anydoor/branch/main/graph/badge.svg)](https://codecov.io/gh/veinkr)

## Features

- **Database Connectors**: ClickHouse, PostgreSQL, SQLite, Spark, Delta Lake, Kafka
- **Configuration Management**: Hydra-based config with datetime resolvers
- **Caching**: Database-backed caching decorator with thread safety
- **Messaging**: Feishu, QYWX (WeChat Work), Telegram integrations
- **Utilities**: Concurrent processing, S3 sync, Vault integration, proxy management
- **Quant Tools**: Binance API, stock market utilities

## Installation

```bash
# Basic installation
pip install anydoor

# With optional dependencies
pip install anydoor[all]           # All extras
pip install anydoor[kafka]         # Kafka support
pip install anydoor[polars]        # Polars + Delta Lake support
```

## Development

### Prerequisites
- Python 3.12+
- uv (Python package manager)

### Setup
```bash
git clone <repository-url>
cd anydoor
uv sync
```

### Common Commands
```bash
make test      # Run tests
make lint      # Run linting
make format    # Format code
make mypy      # Type checking
make build     # Build package
```
