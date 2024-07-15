EXTENSION     = pgmq
EXTVERSION    = $(shell grep "^default_version" pgmq.control | sed -r "s/default_version[^']+'([^']+).*/\1/")
DATA          = $(wildcard sql/*--*.sql)
TESTS         = $(wildcard test/sql/*.sql)
REGRESS       = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS ?= --inputdir=test
EXTRA_CLEAN   = $(EXTENSION)-$(EXTVERSION).zip sql/$(EXTENSION)--$(EXTVERSION).sql META.json Trunk.toml

# pg_isolation_regress available in v14 and higher.
ifeq ($(shell test $$(pg_config --version | awk '{print $$2}' | awk 'BEGIN { FS = "." }; { print $$1 }') -ge 14; echo $$?),0)
ISOLATION   = $(patsubst test/specs/%.spec,%,$(wildcard test/specs/*.spec))
ISOLATION_OPTS = $(REGRESS_OPTS)
endif

PG_CONFIG   ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: sql/$(EXTENSION)--$(EXTVERSION).sql Trunk.toml

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

dist: Trunk.toml META.json
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ -o $(EXTENSION)-$(EXTVERSION).zip --add-file META.json --add-file Trunk.toml HEAD sql pgmq.control README.md UPDATING.md Makefile

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest

pgxn-zip: dist

META.json:
	sed 's/@@VERSION@@/$(EXTVERSION)/g' META.json.in > META.json

Trunk.toml:
	sed 's/@@VERSION@@/$(EXTVERSION)/g' Trunk.toml.in > Trunk.toml

install-pg-partman:
	git clone https://github.com/pgpartman/pg_partman.git && \
	cd pg_partman && \
	git checkout v4.7.4 && \
	make && \
	make install PG_CONFIG=$(PG_CONFIG) && \
	cd ../ && rm -rf pg_partman
