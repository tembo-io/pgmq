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

## Installing Postgres

If you already have Postgres instaleld locally, you can skip to [Install PGMQ to Postgres](#install-pgmq-to-postgres).

If you need to install Postgres or want to set up a new environment for PGMQ development, [pgenv](https://github.com/theory/pgenv/) is a command line utility that makes it very easy to install and manage multiple versions of Postgres.
 Follow the [installation instructions](https://github.com/theory/pgenv/?tab=readme-ov-file#installation) to install it.
 If you are on MacOS, you may need link `brew link icu4c --force` in order to successfully build Postgres.

Install Postgres 16.3

```bash
pgenv build 16.3
pgenv use 16.3
```

Connect to Postgres:

```bash
psql -U postgres
```

A fresh install will have not have PGMQ installed.

```psql
postgres=# \dx
                 List of installed extensions
  Name   | Version |   Schema   |         Description          
---------+---------+------------+------------------------------
 plpgsql | 1.0     | pg_catalog | PL/pgSQL procedural language
(1 row)
```

## Installing PGMQ

Clone the repo and change into the directory.

```bash
git clone https://github.com/tembo-io/pgmq.git
cd pgmq
```

### Install PGMQ to Postgres

This will install the extension to the Postgres using the `pg_config` that is currently on your `PATH`. If you have multiple versions of Postgres installed, make sure you are using the correct installation by running `make install PG_CONFIG=/path/to/pg_config`.

```bash
make
make install
```

Finally, you can create the extension and get started with the example in the [README.md](README.md#sql-examples).

```psql
CREATE EXTENSION pgmq cascade;
```

### Installing pg_partman (optional)

If you are working with partitioned queues, you will need to install `pg_partman`.

```bash
make install-pg-partman
```

## Running tests

Tests are written in Rust, so you will need to have the [Rust toolchain](https://www.rust-lang.org/tools/install) installed in order to run them.

Once you have a postgres instance with the extension installed, run:

```bash
make test
```

## Releases

PGMQ Postgres Extension releases are automated through two Github workflows; [Containers / Trunk packages](https://github.com/tembo-io/pgmq/blob/main/.github/workflows/extension_ci.ym), and [PGXN distribution](https://github.com/tembo-io/pgmq/blob/main/.github/workflows/pgxn-release.yml). To create a release:

1. Update and commit the new valid [semver](https://semver.org/) version in [pgmq.control](https://github.com/tembo-io/pgmq/blob/main/pgmq.control).
2. Create a [Github release](https://github.com/tembo-io/pgmq/releases) using the extension's version for the `tag` and `title`. Auto-generate the release notes and/or add more relevant details as needed.

### Container Images

Postgres images with PGMQ and all required dependencies are built and published to `quay.io/tembo-pg{PG_VERSION}-pgmq:{TAG}` for all supported Postgres versions and PGMQ releases.

### Extension Packages

The required extension files are publish to and hosted at [pgt.dev](https://pgt.dev/extensions/pgmq) and [PGXN](https://pgxn.org/dist/pgmq/).

### Client SDKs

See subdirectories for the [Rust](https://github.com/tembo-io/pgmq/tree/main/core) and [Python](https://github.com/tembo-io/pgmq/tree/main/tembo-pgmq-python) SDK release processes.
