POSTGRES_PASSWORD:=postgres
DATABASE_URL:=postgres://postgres:postgres@0.0.0.0:5432/postgres
EXTENSION_DB:=postgres://postgres:postgres@0.0.0.0:5432/pgmq_ext_test

format:
	cargo sqlx prepare --database-url ${DATABASE_URL}
	cargo +nightly fmt --all
	cargo clippy

update.readme:
	cargo readme \
		--no-title \
		--no-indent-headings \
		> README.md

run.postgres:
	docker run --rm -d --name pgmq-pg -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 quay.io/tembo/pg17-pgmq:latest

test: run.postgres
	sleep 4;
	echo "Running all tests..."
	sqlx migrate run --database-url ${DATABASE_URL}
	psql ${DATABASE_URL} -c "CREATE DATABASE pgmq_ext_test;"
	sqlx migrate run --database-url ${EXTENSION_DB}
	cargo test

test.cleanup:
	docker stop pgmq-pg

setup.env:
	sqlx migrate run --database-url ${DATABASE_URL}
