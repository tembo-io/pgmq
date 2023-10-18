# Postgres Message Queue (PGMQ)

A lightweight message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

[![Static Badge](https://img.shields.io/badge/%40tembo-community?logo=slack&label=slack)](https://join.slack.com/t/tembocommunity/shared_invite/zt-20dtnhcmo-pLNV7_Aobi50TdTLpfQ~EQ)

## Features

- Lightweight - No background worker or external dependencies, just Postgres functions packaged in an extension
- Guaranteed "exactly once" delivery of messages to a consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- Messages stay in the queue until explicitly removed
- Messages can be archived, instead of deleted, for long-term retention and replayability

## Support
Postgres 12-16.

## Table of Contents
- [Postgres Message Queue (PGMQ)](#postgres-message-queue-pgmq)
  - [Features](#features)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Client Libraries](#client-libraries)
  - [SQL Examples](#sql-examples)
    - [Creating a queue](#creating-a-queue)
    - [Send two messages](#send-two-messages)
    - [Read messages](#read-messages)
    - [Pop a message](#pop-a-message)
    - [Archive a message](#archive-a-message)
    - [Delete a message](#delete-a-message)
    - [Drop a queue](#drop-a-queue)
- [Configuration](#configuration)
  - [Partitioned Queues](#partitioned-queues)
  - [Visibility Timeout (vt)](#visibility-timeout-vt)
  - [✨ Contributors](#-contributors)

## Installation

The fastest way to get started is by running the Tembo docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

If you'd like to build from source, you can follow the instructions in [CONTRIBUTING.md](CONTRIBUTING.md).

## Client Libraries

- [Rust](https://github.com/tembo-io/pgmq/tree/main/pgmq-rs)
- [Python](https://github.com/tembo-io/pgmq/tree/main/tembo-pgmq-python)

Community

- [Go](https://github.com/craigpastro/pgmq-go)

## SQL Examples


```bash
# Connect to Postgres
psql postgres://postgres:postgres@0.0.0.0:5432/postgres
```

```sql
-- create the extension
CREATE EXTENSION pgmq;
```

### Creating a queue

Every queue is its own table in Postgres. The table name is the queue name prefixed with `q_`.
For example, `q_my_queue` is the table for the queue `my_queue`.

```sql
-- creates the queue
SELECT pgmq.create('my_queue');
```

```text
 create
-------------

(1 row)
```

### Send two messages

```sql
-- messages are sent as JSON
SELECT * from pgmq.send('my_queue', '{"foo": "bar1"}');
SELECT * from pgmq.send('my_queue', '{"foo": "bar2"}');
```

The message id is returned from the send function.

```text
 send
-----------
         1
(1 row)

 send
-----------
         2
(1 row)
```

### Read messages

Read `2` message from the queue. Make them invisible for `30` seconds.
 If the messages are not deleted or archived within 30 seconds, they will become visible again
    and can be read by another consumer.

```sql
SELECT pgmq.read('my_queue', 30, 2);
```

```text
 msg_id | read_ct |          enqueued_at          |              vt               |     message
--------+---------+-------------------------------+-------------------------------+-----------------
      1 |       1 | 2023-08-16 08:37:54.567283-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar1"}
      2 |       1 | 2023-08-16 08:37:54.572933-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar2"}
```

If the queue is empty, or if all messages are currently invisible, no rows will be returned.

```sql
SELECT pgmq.read('my_queue', 30, 1);
```

```text
 msg_id | read_ct | enqueued_at | vt | message
--------+---------+-------------+----+---------
```

### Pop a message

```sql
-- Read a message and immediately delete it from the queue. Returns `None` if the queue is empty.
SELECT pgmq.pop('my_queue');
```

```text
 msg_id | read_ct |          enqueued_at          |              vt               |     message
--------+---------+-------------------------------+-------------------------------+-----------------
      1 |       1 | 2023-08-16 08:37:54.567283-05 | 2023-08-16 08:38:29.989841-05 | {"foo": "bar1"}
```

### Archive a message

Archiving a message removes it from the queue, and inserts it to the archive table.

Archive message with msg_id=2.

```sql
SELECT pgmq.archive('my_queue', 2);
```

```text
 archive
--------------
 t
(1 row)
```

Archive tables have the prefix `a_`:
```sql
SELECT pgmq.a_my_queue;
```

```text
 msg_id | read_ct |         enqueued_at          |          archived_at          |              vt               |     message
--------+---------+------------------------------+-------------------------------+-------------------------------+-----------------
      2 |       1 | 2023-04-25 00:55:40.68417-05 | 2023-04-25 00:56:35.937594-05 | 2023-04-25 00:56:20.532012-05 | {"foo": "bar2"}```
```

### Delete a message

Send another message, so that we can delete it.

```sql
SELECT pgmq.send('my_queue', '{"foo": "bar3"}');
```

```text
 send
-----------
        3
(1 row)
```

Delete the message with id `3` from the queue named `my_queue`.

```sql
SELECT pgmq.delete('my_queue', 3);
```

```text
 delete
-------------
 t
(1 row)
```

### Drop a queue

Delete the queue `my_queue`.

```sql
SELECT pgmq.drop_queue('my_queue');
```

```text
 drop_queue
-----------------
 t
(1 row)
```

# Configuration

## Partitioned Queues

You will need to install [pg_partman](https://github.com/pgpartman/pg_partman/) if you want to use `pgmq` partitioned queues.

`pgmq` queue tables can be created as a partitioned table by using `pgmq.create_partitioned()`. [pg_partman](https://github.com/pgpartman/pg_partman/)
handles all maintenance of queue tables. This includes creating new partitions and dropping old partitions.

Partitions behavior is configured at the time queues are created, via `pgmq.create_partitioned()`. This function has three parameters:

`queue_name: text`: The name of the queue. Queues are Postgres tables prepended with `q_`. For example, `q_my_queue`. The archive is instead prefixed by `a_`, for example `a_my_queue`.


`partition_interval: text` - The interval at which partitions are created. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, queues are partitioned by the time at which messages are sent to the table (`enqueued_at`). A value of `'daily'` would create a new partition each day. When it is an integer value, queues are partitioned by the `msg_id`. A value of `'100'` will create a new partition every 100 messages. The value must agree with `retention_interval` (time based or numeric). The default value is `daily`.


`retention_interval: text` - The interval for retaining partitions. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, partitions containing data greater than the duration will be dropped. When it is an integer value, any messages that have a `msg_id` less than `max(msg_id) - retention_interval` will be dropped. For example, if the max `msg_id` is 100 and the `retention_interval` is 60, any partitions with `msg_id` values less than 40 will be dropped. The value must agree with `partition_interval` (time based or numeric). The default is `'5 days'`. Note: `retention_interval` does not apply to messages that have been deleted via `pgmq.delete()` or archived with `pgmq.archive()`. `pgmq.delete()` removes messages forever and `pgmq.archive()` moves messages to the corresponding archive table forever (for example, `a_my_queue`).


In order for automatic partition maintenance to take place, several settings must be added to the `postgresql.conf` file, which is typically located in the postgres `DATADIR`.
 `pg_partman_bgw.interval`
in `postgresql.conf`. Below are the default configuration values set in Tembo docker images.

Add the following to `postgresql.conf`. Note, changing `shared_preload_libraries` requires a restart of Postgres.

`pg_partman_bgw.interval` sets the interval at which `pg_partman` conducts maintenance. This creates new partitions and dropping of partitions falling out of the `retention_interval`. By default, `pg_partman` will keep 4 partitions "ahead" of the currently active partition.

```
shared_preload_libraries = 'pg_partman_bgw' # requires restart of Postgres
pg_partman_bgw.interval = 60
pg_partman_bgw.role = 'postgres'
pg_partman_bgw.dbname = 'postgres'
```


## Visibility Timeout (vt)

pgmq guarantees exactly once delivery of a message within a visibility timeout. The visibility timeout is the amount of time a message is invisible to other consumers after it has been read by a consumer. If the message is NOT deleted or archived within the visibility timeout, it will become visible again and can be read by another consumer. The visibility timeout is set when a message is read from the queue, via `pgmq.read()`. It is recommended to set a `vt` value that is greater than the expected time it takes to process a message. After the application successfully processes the message, it should call `pgmq.delete()` to completely remove the message from the queue or `pgmq.archive()` to move it to the archive table for the queue.

## ✨ Contributors

Thanks goes to these incredible people:

<a href="https://github.com/tembo-io/pgmq/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=tembo-io/pgmq" />
</a>
