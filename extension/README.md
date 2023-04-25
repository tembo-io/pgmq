# Postgres Message Queue (PGMQ)

## Installation

The fastest way to get started is by running the CoreDB docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/postgres:latest
```

# Development

Setup `pgrx`.

```bash
cargo install --locked cargo-pgrx
cargo pgrx init
```

Then, clone this repo and change into this directory.

```bash
git clone git@github.com:CoreDB-io/coredb.git
cd coredb/extensions/pgmq/
```

### Setup dependencies

Install:
- [pg_partman](https://github.com/pgpartman/pg_partman), which is required for partitioned tables.


Update postgresql.conf in the development environment.
```
#  ~/.pgx/data-14/postgresql.conf
shared_preload_libraries = 'pg_partman_bgw'
```


Run the dev environment

```bash
cargo pgrx run pg15
```

Create the extension

```pql
CREATE EXTENSION pgmq cascade;
```

# Packaging

Run this script to package into a `.deb` file, which can be installed on Ubuntu.

```
/bin/bash build-extension.sh
```
