build-wheel:
	poetry build -f wheel

test:
	poetry install
	poetry run pytest