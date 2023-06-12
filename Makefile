format:
	cargo +nightly fmt --all
	cargo +nightly clippy

run.postgres:
	docker run --rm -d --name pgmq-pg -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 quay.io/coredb/pgmq-pg:latest
