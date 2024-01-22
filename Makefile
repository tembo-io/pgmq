PGRX_POSTGRES ?= pg15
DISTNAME = $(shell grep -m 1 '^name' Cargo.toml | sed -e 's/[^"]*"\([^"]*\)",\{0,1\}/\1/')
DISTVERSION  = $(shell grep -m 1 '^version' Cargo.toml | sed -e 's/[^"]*"\([^"]*\)",\{0,1\}/\1/')

test:
	cargo pgrx test $(PGRX_POSTGRES)
	cargo test --no-default-features --features ${PGRX_POSTGRES} -- --test-threads=1 --ignored

format:
	cargo +nightly fmt --all
	cargo +nightly clippy

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest

META.json: META.json.in Cargo.toml
	@sed "s/@CARGO_VERSION@/$(DISTVERSION)/g" $< > $@

$(DISTNAME)-$(DISTVERSION).zip: META.json
	git archive --format zip --prefix $(DISTNAME)-$(DISTVERSION)/ --add-file $< -o $(DISTNAME)-$(DISTVERSION).zip HEAD

pgxn-zip: $(DISTNAME)-$(DISTVERSION).zip

clean:
	@rm -rf META.json $(DISTNAME)-$(DISTVERSION).zip
