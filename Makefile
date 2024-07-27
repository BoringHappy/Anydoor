build-wheel:
	poetry build -f wheel

test:
	poetry install
	poetry run pytest

export-requirements:
	poetry export -f requirements.txt --output requirements.txt
	pip install -r requirements.txt