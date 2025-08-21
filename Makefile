bump:
	uv version --bump patch

install:
	uv sync

test:
	pytest tests/ -v

lint:
	ruff check anydoor/

mypy:
	mypy anydoor/

mypy-strict:
	mypy anydoor/ --strict

mypy-install-stubs:
	mypy --install-types

format:
	ruff format anydoor/

build:
	uv build
