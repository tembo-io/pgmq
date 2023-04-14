SCOPE=src/

format:
	poetry run black ${SCOPE}
	poetry run isort --atomic .

lints:
	poetry run black --check ${SCOPE}
	poetry run isort --check-only ${SCOPE}
	poetry run flake8 ${SCOPE}
	poetry run mypy ${SCOPE}