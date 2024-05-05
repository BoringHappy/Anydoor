build-wheel:
	poetry build -f wheel

test:
	poetry run pytest