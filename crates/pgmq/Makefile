POSTGRES_PASSWORD:=postgres

update.readme:
	cargo readme \
		--no-title \
		--no-indent-headings \
		> README.md

run.docker:
	docker run --rm -d --name postgres -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 postgres:15.1

test.integration: run.docker
	sleep 2;
	cargo test --test integration_test

test.cleanup:
	docker stop postgres
