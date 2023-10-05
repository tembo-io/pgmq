PGRX_POSTGRES ?= pg15

test:
	cargo pgrx test $(PGRX_POSTGRES)
	cargo test --no-default-features --features ${PGRX_POSTGRES} -- --test-threads=1 --ignored

format:
	cargo +nightly fmt --all
	cargo +nightly clippy

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest
