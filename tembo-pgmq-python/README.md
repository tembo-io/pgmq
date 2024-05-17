# Tembo's Python Client for PGMQ

## Installation

Install with `pip` from pypi.org:

```bash
pip install tembo-pgmq-python
```

Dependencies:

Postgres running the [Tembo PGMQ extension](https://github.com/tembo-io/tembo/tree/main/pgmq).

## Usage

### Start a Postgres Instance with the Tembo extension installed

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

### Initialize a connection to Postgres

```python
from tembo_pgmq_python import PGMQueue, Message

queue = PGMQueue(host="0.0.0.0")
```

### Create a queue 

```python
queue.create_queue("my_queue")
```
### or a partitioned queue
```python
queue.create_partitioned_queue("my_partitioned_queue", partition_size=10000)
```
### Send a message

```python
msg_id: int = queue.send("my_queue", {"hello": "world"})
```

### Send a batch of messages

```python
msg_ids: list[int] = queue.send_batch("my_queue", [{"hello": "world"}, {"foo": "bar"}])
```

### Read a message, set it invisible for 30 seconds

```python
read_message: Message = queue.read("my_queue", vt=30)
print(read_message)
```

### Read a batch of messages

```python
read_messages: list[Message] = queue.read_batch("my_queue", vt=30, batch_size=5)
for message in read_messages:
    print(message)
```

### Archive the message after we're done with it. Archived messages are moved to an archive table

```python
archived: bool = queue.archive("my_queue", read_message.msg_id)
```

### Delete a message completely

```python
msg_id: int = queue.send("my_queue", {"hello": "world"})
read_message: Message = queue.read("my_queue")
deleted: bool = queue.delete("my_queue", read_message.msg_id)
```

### Pop a message, deleting it and reading it in one transaction

```python
popped_message: Message = queue.pop("my_queue")
print(popped_message)
```

### Purge all messages from a queue

```python
purged_count: int = queue.purge("my_queue")
print(f"Purged {purged_count} messages from the queue.")
```
