# Postgres Message Queue (PGMQ)

A lightweight distributed message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

## Features

- Lightweight - Built with Rust and Postgres only
- Guaranteed delivery of messages to `exactly one` consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- Messages stay in the queue until deleted
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Table maintenance (bloat) automated with [pg_partman]()
- High performance operations with index-only scans.
  
## Table of Contents
- [Postgres Message Queue (PGMQ)](#postgres-message-queue-pgmq)
  - [Features](#features)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Client Libraries](#client-libraries)
  - [SQL Examples](#sql-examples)
    - [Creating a queue](#creating-a-queue)
    - [Send two message](#send-two-message)
    - [Read messages](#read-messages)
    - [Pop a message](#pop-a-message)
    - [Archive a message](#archive-a-message)
    - [Delete a message](#delete-a-message)
- [Development](#development)
    - [Setup dependencies](#setup-dependencies)
- [Packaging](#packaging)
- [Configuration](#configuration)
  - [Partitioned Queues](#partitioned-queues)

## Installation

The fastest way to get started is by running the CoreDB docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/postgres:latest
```

## Client Libraries


- [Rust](https://github.com/CoreDB-io/coredb/tree/main/pgmq/core)
- [Python](https://github.com/CoreDB-io/coredb/tree/main/pgmq/coredb-pgmq-python)

## SQL Examples


```bash
# Connect to Postgres
psql postgres://postgres:postgres@0.0.0.0:5432/postgres
```

```sql
-- create the extension, pg_partman is also required
CREATE EXTENSION pgmq CASCADE;
```

### Creating a queue

```sql
-- creates the queue.
SELECT pgmq_create('my_queue');

 pgmq_create
-------------
```

### Send two message

```sql
-- messages are sent as JSON
pgmq=# 
SELECT * from pgmq_send('my_queue', '{"foo": "bar1"}');
SELECT * from pgmq_send('my_queue', '{"foo": "bar2"}');
```

```sql
-- the message id is returned from the send function
 pgmq_send 
-----------
         1
(1 row)

 pgmq_send 
-----------
         2
(1 row)
```

### Read messages

Read two message from the queue. Make them invisible for 30 seconds.

```sql
-- parameters are queue name, visibility timeout, and number of messages to read
pgmq=# SELECT * from pgmq_read('my_queue', 30, 2);

 msg_id | read_ct |              vt               |          enqueued_at          |    message
--------+---------+-------------------------------+-------------------------------+---------------
      1 |       1 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar"}
      2 |       1 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar"}
```

If the queue is empty, or if all messages are currently invisible, no rows will be returned.

```sql
pgmq=# SELECT * from pgmq_read('my_queue', 30, 1);
 msg_id | read_ct | vt | enqueued_at | message
--------+---------+----+-------------+---------
```

### Pop a message


```sql
-- Read a message and immediately delete it from the queue. Returns `None` if the queue is empty.
pgmq=# SELECT * from pgmq_pop('my_queue');

 msg_id | read_ct |              vt               |          enqueued_at          |    message
--------+---------+-------------------------------+-------------------------------+---------------
      1 |       2 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar"}
```

### Archive a message


```sql
-- Archiving a message removes it from the queue, and inserts it to the archive table.
-- TODO: implement this in the extension

```

### Delete a message

```sql
-- Delete a message id `1` from queue named `my_queue`.
pgmq=# select pgmq_delete('my_queue', 1);
 pgmq_delete
-------------
 t
 ```

# Development

Setup `pgx`.

```bash
cargo install --locked cargo-pgx
cargo pgx init
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
cargo pgx run pg14
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

# Configuration

## Partitioned Queues

pgmq supports partitioned queues with `create_partitioned()` by `msg_id` and the default value is 10000 messages per partition.
New partitions are automatically created by [pg_partman](https://github.com/pgpartman/pg_partman/),
 which is a dependency of this extension. New partitions are created, if necessary, by setting `pg_partman_bgw.interval` 
 in `postgresql.conf`. Below are the default configuration values set in CoreDB docker images.
