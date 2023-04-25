SCOPE=coredb_pgmq_python/

format:
	poetry run black ${SCOPE}
	poetry run isort --atomic .

lints:
	poetry run black --check ${SCOPE}
	poetry run isort --check-only ${SCOPE}
	poetry run flake8 ${SCOPE}
	poetry run mypy ${SCOPE}

run-pgmq-postgres:
	docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/coredb-pg:latest || true

test: run-pgmq-postgres
	poetry run pytest
