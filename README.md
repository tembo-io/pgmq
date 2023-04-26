# Postgres Message Queue (PGMQ)

A lightweight distributed message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

## Features

- Lightweight - Built with Rust and Postgres only
- Guaranteed "exactly once" delivery of messages consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- Messages stay in the queue until deleted
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Table (bloat) maintenance automated with [pg_partman](https://github.com/pgpartman/pg_partman)
- High performance operations with index-only scans.
  
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
- [Configuration](#configuration)
  - [Partitioned Queues](#partitioned-queues)
  - [Visibility Timeout (vt)](#visibility-timeout-vt)

## Installation

The fastest way to get started is by running the CoreDB docker image, where PGMQ comes pre-installed.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/pgmq-pg:latest
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

Every queue is its own table in Postgres. The table name is the queue name prefixed with `pgmq_`.
 For example, `pgmq_my_queue` is the table for the queue `my_queue`.

Optionally, the `partition_interval` and `retention_interval` can be configured. See [Configuration](#configuration).
```sql
-- creates the queue

-- params
-- queue_name: text
-- partition_interval: text DEFAULT 'daily'::text
-- retention_interval: text DEFAULT '5 days'::text
SELECT pgmq_create('my_queue');

 pgmq_create
-------------
```

### Send two messages

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

Read `2` message from the queue. Make them invisible for `30` seconds. 
 If the messages are not deleted or archived within 30 seconds, they will become visible again
    and can be read by another consumer.

```sql
pgmq=# SELECT * from pgmq_read('my_queue', 30, 2);

 msg_id | read_ct |              vt               |          enqueued_at          |    message
--------+---------+-------------------------------+-------------------------------+---------------
      1 |       1 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar1"}
      2 |       1 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar2"}
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
      1 |       2 | 2023-02-07 04:56:00.650342-06 | 2023-02-07 04:54:51.530818-06 | {"foo":"bar1"}
```

### Archive a message


```sql
-- Archiving a message removes it from the queue, and inserts it to the archive table.
-- archive message with msg_id=2
pgmq=# SELECT * from pgmq_archive('my_queue', 2);
```
```sql
pgmq=#  SELECT * from pgmq_my_queue_archive;
 msg_id | read_ct |         enqueued_at          |          deleted_at           |              vt               |     message     
--------+---------+------------------------------+-------------------------------+-------------------------------+-----------------
      2 |       1 | 2023-04-25 00:55:40.68417-05 | 2023-04-25 00:56:35.937594-05 | 2023-04-25 00:56:20.532012-05 | {"foo": "bar2"}```
```

### Delete a message

```sql
-- Delete a message id `3` from queue named `my_queue`.
pgmq=# SELECT * from pgmq_send('my_queue', '{"foo": "bar3"}');
pgmq=# SELECT pgmq_delete('my_queue', 3);
 pgmq_delete
-------------
 t
```

# Configuration

## Partitioned Queues

`pgmq` queue tables are partitioned by default. [pg_partman](https://github.com/pgpartman/pg_partman/)
handles all maintenance of queue tables. This includes creating new partitions and dropping old partitions.

Partitions behavior is configured at the time queues are created, via `pgmq_create()`. This function has a three parameters:

`queue_name: text` : The name of the queue. Queues are Postgres tables prepended with `pgmq_`. For example, `pgmq_my_queue`.


`partition_interval: text` - The interval at which partitions are created. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, queues are partitioned by the time at which messages are sent to the table (`enqueued_at`). A value of `daily'` would create a new partition each day. When it is an integer value, queues are partitioned by the `msg_id`. A value of `'100'` will create a new partition every 100 messages. The value must agree with `retention_interval` (time based or numeric). The default value is `daily`.


`retention_interval: text` - The interval for retaining partitions. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, partitions containing data greater than the duration will be dropped. When it is an integer value,any messages that have a `msg_id` less than `max(msg_id) - retention_interval` will be dropped. For example, if the max `msg_id` is 100 and the `retention_interval` is 60, any partitions with `msg_id` values less than 40 will be dropped. The value must agree with `partition_interval` (time based or numeric). The default is `'5 days'`. Note: `retention_interval` does not apply to messages that have been deleted via `pgmq_delete()` or archived with `pgmq_archive()`. `pgmq_delete()` removes messages forever and `pgmq_archive()` moves messages to a the corresponding archive table forever (for example, `pgmq_my_queue_archive`).


In order for automatic partition maintenance to take place, several settings must be added to the `postgresql.conf` file, which is typically located in the postgres `DATADIR`.
 `pg_partman_bgw.interval` 
in `postgresql.conf`. Below are the default configuration values set in CoreDB docker images.

Add the following to `postgresql.conf`. Note, changing `shared_preload_libraries` requires a restart of Postgres.

`pg_partman_bgw.interval` sets the interval at which `pg_partman` conducts maintenance. This creates new partitions and dropping of partitions falling out of the `retention_interval`. By default, `pg_partman` will keep 4 partitions "ahead" of the currently active partition.

```
shared_preload_libraries = 'pg_partman_bgw' # requires restart of Postgrs
pg_partman_bgw.interval = 60
pg_partman_bgw.role = 'postgres'
pg_partman_bgw.dbname = 'postgres'
```


## Visibility Timeout (vt)

pgmq guarantees exactly once delivery of a message within a visibility timeout. The visibility timeout is the amount of time a message is invisible to other consumers after it has been read by a consumer. If the message is NOT deleted or archived within the visibility timeout, it will become visible again and can be read by another consumer. The visibility timeout is set when a message is read from the queue, via `pgmq_read()`. It is recommended to set a `vt` value that is greater than the expected time it takes to process a message. After the application successfully processes the message, it should call `pgmq_delete()` to completely remove the message from the queue or `pgmq_archive()` to move it to the archive table for the queue.
