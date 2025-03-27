# Tembo's Python Client for PGMQ

## Installation

Install with `pip` from pypi.org:

```bash
pip install tembo-pgmq-python
```

To use the async version, install with the optional dependencies:

```bash
pip install tembo-pgmq-python[async]
```

Dependencies:

- Postgres running the [Tembo PGMQ extension](https://github.com/tembo-io/tembo/tree/main/pgmq).

## Usage

### Start a Postgres Instance with the Tembo extension installed

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg17-pgmq:latest
```

### Using Environment Variables

Set environment variables:

```bash
export PG_HOST=127.0.0.1
export PG_PORT=5432
export PG_USERNAME=postgres
export PG_PASSWORD=postgres
export PG_DATABASE=test_db
```

Initialize a connection to Postgres using environment variables:

```python
from tembo_pgmq_python import PGMQueue, Message

queue = PGMQueue()
```

### Note on the async version

Initialization for the async version requires an explicit call of the initializer:

```python
from tembo_pgmq_python.async_queue import PGMQueue

async def main():
    queue = PGMQueue()
    await queue.init()
```

Then, the interface is exactly the same as the sync version.

### Initialize a connection to Postgres without environment variables

```python
from tembo_pgmq_python import PGMQueue, Message

queue = PGMQueue(
    host="0.0.0.0",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres"
)
```

### Create a queue 

```python
queue.create_queue("my_queue")
```

### Or create a partitioned queue

```python
queue.create_partitioned_queue("my_partitioned_queue", partition_interval=10000)
```

### List all queues

```python
queues = queue.list_queues()
for q in queues:
    print(f"Queue name: {q}")
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

### Read messages with polling

The `read_with_poll` method allows you to repeatedly check for messages in the queue until either a message is found or the specified polling duration is exceeded. This can be useful in scenarios where you want to wait for new messages to arrive without continuously querying the queue in a tight loop.

In the following example, the method will check for up to 5 messages in the queue `my_queue`, making the messages invisible for 30 seconds (`vt`), and will poll for a maximum of 5 seconds (`max_poll_seconds`) with intervals of 100 milliseconds (`poll_interval_ms`) between checks.

```python
read_messages: list[Message] = queue.read_with_poll(
    "my_queue", vt=30, qty=5, max_poll_seconds=5, poll_interval_ms=100
)
for message in read_messages:
    print(message)
```

This method will continue polling until it retrieves any messages, with a maximum of (`qty`) messages in a single poll, or until the `max_poll_seconds` duration is reached. The `poll_interval_ms` parameter controls the interval between successive polls, allowing you to avoid hammering the database with continuous queries.

### Archive the message after we're done with it

Archived messages are moved to an archive table.

```python
archived: bool = queue.archive("my_queue", read_message.msg_id)
```

### Archive a batch of messages

```python
archived_ids: list[int] = queue.archive_batch("my_queue", [msg_id1, msg_id2])
```

### Delete a message completely

```python
read_message: Message = queue.read("my_queue")
deleted: bool = queue.delete("my_queue", read_message.msg_id)
```

### Delete a batch of messages

```python
deleted_ids: list[int] = queue.delete_batch("my_queue", [msg_id1, msg_id2])
```

### Set the visibility timeout (VT) for a specific message

```python
updated_message: Message = queue.set_vt("my_queue", msg_id, 60)
print(updated_message)
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

### Detach an archive from a queue

```python
queue.detach_archive("my_queue")
```

### Drop a queue

```python
dropped: bool = queue.drop_queue("my_queue")
print(f"Queue dropped: {dropped}")
```

### Validate the length of a queue name

```python
queue.validate_queue_name("my_queue")
```

### Get queue metrics

The `metrics` method retrieves various statistics for a specific queue, such as the queue length, the age of the newest and oldest messages, the total number of messages, and the time of the metrics scrape.

```python
metrics = queue.metrics("my_queue")
print(f"Metrics: {metrics}")
```

### Access individual metrics

You can access individual metrics directly from the `metrics` method's return value:

```python
metrics = queue.metrics("my_queue")
print(f"Queue name: {metrics.queue_name}")
print(f"Queue length: {metrics.queue_length}")
print(f"Newest message age (seconds): {metrics.newest_msg_age_sec}")
print(f"Oldest message age (seconds): {metrics.oldest_msg_age_sec}")
print(f"Total messages: {metrics.total_messages}")
print(f"Scrape time: {metrics.scrape_time}")
```

### Get metrics for all queues

The `metrics_all` method retrieves metrics for all queues, allowing you to iterate through each queue's metrics.

```python
all_metrics = queue.metrics_all()
for metrics in all_metrics:
    print(f"Queue name: {metrics.queue_name}")
    print(f"Queue length: {metrics.queue_length}")
    print(f"Newest message age (seconds): {metrics.newest_msg_age_sec}")
    print(f"Oldest message age (seconds): {metrics.oldest_msg_age_sec}")
    print(f"Total messages: {metrics.total_messages}")
    print(f"Scrape time: {metrics.scrape_time}")
```

### Optional Logging Configuration

You can enable verbose logging and specify a custom log filename.

```python
queue = PGMQueue(
    host="0.0.0.0",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
    verbose=True,
    log_filename="my_custom_log.log"
)
```

# Using Transactions

To perform multiple operations within a single transaction, use the `@transaction` decorator from the `tembo_pgmq_python.decorators` module. 
This ensures that all operations within the function are executed within the same transaction and are either committed together or rolled back if an error occurs.

First, import the transaction decorator:

```python
from tembo_pgmq_python.decorators import transaction
```

### Example: Transactional Operation

```python
@transaction
def transactional_operation(queue: PGMQueue, conn=None):
    # Perform multiple queue operations within a transaction
    queue.create_queue("transactional_queue", conn=conn)
    queue.send("transactional_queue", {"message": "Hello, World!"}, conn=conn)

```
To execute the transaction:

```python
try:
    transactional_operation(queue)
except Exception as e:
    print(f"Transaction failed: {e}")
``` 
In this example, the transactional_operation function is decorated with `@transaction`,  ensuring all operations inside it are part of a single transaction.  If an error occurs, the entire transaction is rolled back automatically.
