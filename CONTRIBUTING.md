# Postgres Message Queue (PGMQ)

## Installation

The fastest way to get started is by running the Tembo docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

# Development

Setup `pgrx`.

```bash
cargo install --locked cargo-pgrx
cargo pgrx init
```

Then, clone this repo and change into this directory.

```bash
git clone git@https://github.com/tembo-io/pgmq.git
cd pgmq
```

### Setup dependencies

Install:
- [pg_partman](https://github.com/pgpartman/pg_partman), which is required for partitioned tables.


Update postgresql.conf in the development environment.
```
#  ~/.pgrx/data-14/postgresql.conf
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

# Releases

PGMQ Postgres Extension releases are automated through a [Github workflow](https://github.com/tembo-io/pgmq/blob/main/.github/workflows/extension_ci.yml). The compiled binaries are publish to and hosted at [pgt.dev](https://pgt.dev). To create a release, create a new tag follow a valid [semver](https://semver.org/), then create a release with the same name. Auto-generate the release notes and/or add more relevant details as needed. See subdirectories for the [Rust](https://github.com/tembo-io/pgmq/tree/main/core) and [Python](https://github.com/tembo-io/pgmq/tree/main/tembo-pgmq-python) SDK release processes.
