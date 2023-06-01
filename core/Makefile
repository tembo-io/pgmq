POSTGRES_PASSWORD:=postgres

format:
	cargo sqlx prepare
	cargo +nightly fmt --all
	cargo clippy

update.readme:
	cargo readme \
		--no-title \
		--no-indent-headings \
		> README.md

run.postgres:
	docker run --rm -d --name pgmq-pg -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 quay.io/coredb/pgmq-pg:latest

test: run.postgres
	sleep 2;
	echo "Running all tests..."
	sqlx migrate run
	DATABASE_URL=postgres://postgres:postgres@0.0.0.0:5432 cargo test

test.cleanup:
	docker stop pgmq-pg

setup.env:
	sqlx migrate run
