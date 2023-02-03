# Postgres Message Queue

A lightweight message queue extension for Postgres.

## Usage

### Installation

Setup `pgx`.
```bash
cargo install --locked cargo-pgx
cargo pgx init
```

Then, clone this repo and change into this directory.

```
git clone git@github.com:CoreDB-io/coredb.git
cd coredb/extensions/pgmq/
```

Run the dev environment
```bash
cargo pgx run pg14
```


```sql
CREATE EXTENSION pgmq;
```

### Creating a queue

```sql
SELECT pgmq_create('my_queue');
```

### Enqueueing a message

```sql
pgmq=# SELECT pgmq_enqueue('my_queue', '{"foo": "bar"}');
 pgmq_enqueue 
--------------
            1
```

## Read a message
Reads a single message from the queue. 
```sql
pgmq=# SELECT pgmq_read('my_queue');
                               pgmq_read                                
------------------------------------------------------------------------
 {"message":{"foo":"bar"},"msg_id":1,"vt":"2023-01-01T13:54:15.646554"}
```

If the queue is empty, or if all messages are currently invisible, it will immediately return None.
```sql
pgmq=# SELECT pgmq_read('my_queue');
 pgmq_read 
-----------
```

### Pop a message
Read a message and immediately delete it from the queue. Returns `None` if the queue is empty.
```sql
pgmq=# SELECT pgmq_pop('my_queue');
                                pgmq_pop                                
------------------------------------------------------------------------
 {"message":{"foo":"bar"},"msg_id":1,"vt":"2023-01-01T13:55:15.967657"}
```

### Delete a message
Delete a message with id `1` from queue named `my_queue`.
```sql
pgmq=# select pgmq_delete('my_queue', 1);
 pgmq_delete 
-------------
 t
 ```

