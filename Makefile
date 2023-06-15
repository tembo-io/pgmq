format:
	cargo +nightly fmt --all
	cargo +nightly clippy

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/pgmq-pg:latest