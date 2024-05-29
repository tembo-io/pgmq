# Contributing to Postgres Message Queue (PGMQ)

## Installation

The fastest way to get started is by running the Tembo docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

## Building from source

PGMQ requires the `postgres-server-dev` package to build. For example, to install
version 14 on ubuntu:

```bash
sudo apt-get install postgres-server-dev-14
```

Clone the repo and change into the directory.

```bash
git clone https://github.com/tembo-io/pgmq.git
cd pgmq
```

### Install to a pre-existing Postgres

```bash
make
make install
```

Finally, you can create the extension and get started with the example in the [README.md](README.md).

```psql
CREATE EXTENSION pgmq cascade;
```

## Running tests
Once you have a postgres instance with the extension installed, run:

```bash
DATABASE_URL=postgres:postgres@localhost:5432/postgres make test
```

# Releases

PGMQ Postgres Extension releases are automated through a [Github workflow](https://github.com/tembo-io/pgmq/blob/main/.github/workflows/extension_ci.yml). The compiled binaries are publish to and hosted at [pgt.dev](https://pgt.dev). To create a release, create a new tag follow a valid [semver](https://semver.org/), then create a release with the same name. Auto-generate the release notes and/or add more relevant details as needed. See subdirectories for the [Rust](https://github.com/tembo-io/pgmq/tree/main/core) and [Python](https://github.com/tembo-io/pgmq/tree/main/tembo-pgmq-python) SDK release processes.
