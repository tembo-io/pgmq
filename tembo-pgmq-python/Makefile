SCOPE=tembo_pgmq_python/
MIN_POETRY_VERSION=1.7.1

.PHONY: check-poetry-version format lints clear-postgres run-pgmq-postgres test

check-poetry-version:
	@poetry_version=$$(poetry --version | awk '{print $$3}'); \
	required_version=$(MIN_POETRY_VERSION); \
	if [ $$(printf '%s\n' "$$required_version" "$$poetry_version" | sort -V | head -n1) != "$$required_version" ]; then \
		echo "Poetry version $$poetry_version is not sufficient. Please install Poetry version >= $(MIN_POETRY_VERSION)."; \
		exit 1; \
	fi

format: check-poetry-version
	poetry run black ${SCOPE}
	poetry run ruff check --silent --fix --exit-zero ${SCOPE}

lints: check-poetry-version
	poetry run ruff check ${SCOPE}
	poetry run black --check ${SCOPE}

clear-postgres:
	docker rm -f pgmq-postgres || true

run-pgmq-postgres:
	docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg17-pgmq:latest || true

test: clear-postgres run-pgmq-postgres check-poetry-version
	sleep 10 # Give PostgreSQL some time to start up
	poetry run python -m unittest discover tests
