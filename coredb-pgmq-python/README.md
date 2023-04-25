# Coredb's Python Client for PGMQ

## Installation

Install with `pip` from pypi.org

```bash
pip install coredb-pgmq-python
```

Dependencies:

Postgres running the [CoreDB PGMQ extension](https://github.com/CoreDB-io/coredb/tree/main/extensions/pgmq).

## Usage

## Start a Postgres Instance with the CoreDB extension installed

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/coredb/pgmq-pg:latest
```

Initialize a connection to Postgres

```python

from coredb_pgmq_python import PGMQueue, Message

queue = PGMQueue(host="0.0.0.0")
```

Create a queue (or a partitioned queue)

```python
queue.create_queue("my_queue")
# queue.create_partitioned_queue("my_partitioned_queue", partition_size=10000)
```


Send a message

```python
msg_id: int = queue.send("my_queue", {"hello": "world"})
```

Read a message, set it invisible for 30 seconds.

```python
read_message: Message = queue.read("my_queue", vt=10)
print(read_message)
```

Archive the message after we're done with it. Archived messages are moved to an archive table.

```python
archived: bool = queue.archive("my_queue", read_message.msg_id)
```

Delete a message completely.

```python
msg_id: int = queue.send("my_queue", {"hello": "world"})
read_message: Message = queue.read("my_queue")
deleted: bool = queue.delete("my_queue", read_message.msg_id)
```

Pop a message, deleting it and reading it in one transaction.

```python
popped_message: Message = queue.pop("my_queue")
print(popped_message)
```
