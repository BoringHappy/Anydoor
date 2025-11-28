bump:
	uv version --bump patch

install:
	uv sync

install-all:
	uv sync --all-extras

install-dev:
	uv sync --all-extras --group dev

test:
	uv sync --all-extras --group dev
	uv run pytest tests/ -v

lint:
	uv run ruff check anydoor/

mypy:
	uv run mypy anydoor/

mypy-strict:
	uv run mypy anydoor/ --strict

mypy-install-stubs:
	uv run mypy --install-types

format:
	uv run ruff format anydoor/

build:
	uv build
