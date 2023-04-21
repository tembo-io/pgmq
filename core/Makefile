POSTGRES_PASSWORD:=postgres

format:
	cargo fmt
	cargo clippy

update.readme:
	cargo readme \
		--no-title \
		--no-indent-headings \
		> README.md

run.docker:
	docker run --rm -d --name postgres -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 postgres:15.1

test: run.docker
	sleep 2;
	echo "Running all tests..."
	cargo test

test.cleanup:
	docker stop postgres
