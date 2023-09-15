SCOPE=tembo_pgmq_python/

format:
	poetry run black ${SCOPE}
	poetry run isort --atomic .

lints:
	poetry run black --check ${SCOPE}
	poetry run isort --check-only ${SCOPE}
	poetry run flake8 ${SCOPE}
	poetry run mypy ${SCOPE}

clear-postgres:
	docker rm -f pgmq-postgres || true

run-pgmq-postgres:
	docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest || true

test: run-pgmq-postgres
	poetry run pytest
