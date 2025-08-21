# Anydoor
Anydoor to any cozy functions and classes. Suitable for data and llm

[![CI](https://github.com/veinkr/anydoor/workflows/CI/badge.svg)](https://github.com/veinkr/anydoor/actions/workflows/ci.yml)
[![Test Coverage](https://codecov.io/gh/veinkr/anydoor/branch/main/graph/badge.svg)](https://codecov.io/gh/veinkr/anydoor)
[![Coverage Status](https://codecov.io/gh/veinkr/anydoor/branch/main/graph/badge.svg?token=)](https://codecov.io/gh/veinkr/anydoor)

## Development

### Prerequisites
- Python 3.12+
- uv (Python package manager)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd anydoor

# Install dependencies
uv sync
```

### Testing
```bash
# Run tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest tests/ -v --cov=anydoor --cov-report=html

# Run linting
uv run ruff check anydoor/

# Run type checking
uv run mypy anydoor/

# Check code formatting
uv run ruff format --check anydoor/
```

### Using Makefile
```bash
# Install dependencies
make install

# Run tests
make test

# Run linting
make lint

# Run type checking
make mypy

# Format code
make format

# Build package
make build
```

## Test Coverage

This project maintains comprehensive test coverage to ensure code quality and reliability. Coverage reports are automatically generated and updated on every CI run.

### Coverage Badges
- **Test Coverage**: Shows current test coverage percentage
- **Coverage Status**: Displays detailed coverage metrics
- **CI Status**: Indicates overall build health

### Coverage Reports
- **HTML Reports**: Generated locally with `uv run pytest --cov=anydoor --cov-report=html`
- **XML Reports**: Automatically uploaded to Codecov for tracking
- **Terminal Output**: Shows missing lines during test runs

## CI/CD

This project uses GitHub Actions for continuous integration. The following checks run automatically on every push and pull request:

- **Unit Tests**: Runs pytest on Python 3.12 with coverage reporting
- **Code Quality**: Runs ruff linting and mypy type checking
- **Code Formatting**: Ensures code follows formatting standards
- **Coverage Reports**: Generates and uploads test coverage reports to Codecov

### Workflow Files
- `.github/workflows/ci.yml` - Main CI workflow for tests, linting, and formatting
- `.github/workflows/dependencies.yml` - Automated dependency updates (weekly)
