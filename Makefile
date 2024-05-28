EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
EXTVERSION   = $(shell grep -m 1 '[[:space:]]\{8\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

DATA 		     = $(wildcard sql/*--*.sql)
TEST_POSTGRES ?= pg16
PG_CONFIG   ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD sql META.json pgmq.control README.md UPDATING.md

test:
	cargo pgrx test $(TEST_POSTGRES)
	cargo test --no-default-features --features ${TEST_POSTGRES} -- --test-threads=1 --ignored

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest

pgxn-zip: dist.zip

clean:
	@rm -rf "$(EXTENSION)-$(DISTVERSION).zip"
	@rm -rf "sql/$(EXTENSION)-$(DISTVERSION).sql"
