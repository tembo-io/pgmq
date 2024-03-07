SCOPE=tembo_pgmq_python/
POETRY_VERSION:=1.7.1

format:
	poetry run black ${SCOPE}
	poetry run ruff check --silent --fix --exit-zero ${SCOPE}

lints:
	poetry run ruff check ${SCOPE}
	poetry run black --check ${SCOPE}

clear-postgres:
	docker rm -f pgmq-postgres || true

run-pgmq-postgres:
	docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest || true

test: run-pgmq-postgres
	poetry run pytest
