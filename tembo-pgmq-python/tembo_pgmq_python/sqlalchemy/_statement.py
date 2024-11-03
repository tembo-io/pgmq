from typing import List
from sqlalchemy import text

from tembo_pgmq_python.sqlalchemy._types import STATEMENT_TYPE


def check_pg_partman_ext() -> STATEMENT_TYPE:
    """Check if pg_partman extension is installed."""
    return text("create extension if not exists pg_partman cascade;"), {}


def create_queue(queue_name: str, unlogged: bool = False) -> STATEMENT_TYPE:
    """Create a new queue."""
    if unlogged:
        return text("select pgmq.create_unlogged(:queue_name);"), {
            "queue_name": queue_name
        }
    else:
        return text("select pgmq.create(:queue_name);"), {"queue_name": queue_name}


def create_partitioned_queue(
    queue_name: str, partition_interval: int = 10000, retention_interval: int = 100000
) -> STATEMENT_TYPE:
    """Create a new partitioned queue."""
    return (
        text(
            "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
        ),
        {
            "queue_name": queue_name,
            "partition_interval": partition_interval,
            "retention_interval": retention_interval,
        },
    )


def validate_queue_name(queue_name: str) -> STATEMENT_TYPE:
    """Validate the length of a queue name."""
    return text("select pgmq.validate_queue_name(:queue_name);"), {
        "queue_name": queue_name
    }


def drop_queue(queue: str, partitioned: bool = False) -> STATEMENT_TYPE:
    """Drop a queue."""
    return text("select pgmq.drop_queue(:queue, :partitioned);"), {
        "queue": queue,
        "partitioned": partitioned,
    }


def list_queues() -> STATEMENT_TYPE:
    """List all queues."""
    return text("select queue_name from pgmq.list_queues();"), {}


def send(queue_name: str, message: str, delay: int = 0) -> STATEMENT_TYPE:
    """Send a message to a queue."""
    return text(f"select * from pgmq.send('{queue_name}',{message},{delay});")


def send_batch(queue_name: str, messages: str, delay: int = 0) -> STATEMENT_TYPE:
    """Send a batch of messages to a queue."""
    return text(f"select * from pgmq.send_batch('{queue_name}',{messages},{delay});")


def read(queue_name: str, vt: int) -> STATEMENT_TYPE:
    """Read a message from a queue."""
    return text("select * from pgmq.read(:queue_name, :vt, 1);"), {
        "queue_name": queue_name,
        "vt": vt,
    }


def read_batch(queue_name: str, vt: int, batch_size: int) -> STATEMENT_TYPE:
    """Read a batch of messages from a queue."""
    return text("select * from pgmq.read(:queue_name, :vt, :batch_size);"), {
        "queue_name": queue_name,
        "vt": vt,
        "batch_size": batch_size,
    }


def read_with_poll(
    queue_name: str, vt: int, qty: int, max_poll_seconds: int, poll_interval_ms: int
) -> STATEMENT_TYPE:
    """Read messages from a queue with polling."""
    return (
        text(
            "select * from pgmq.read_with_poll(:queue_name, :vt, :qty, :max_poll_seconds, :poll_interval_ms);"
        ),
        {
            "queue_name": queue_name,
            "vt": vt,
            "qty": qty,
            "max_poll_seconds": max_poll_seconds,
            "poll_interval_ms": poll_interval_ms,
        },
    )


def pop(queue_name: str) -> STATEMENT_TYPE:
    """Pop a message from a queue."""
    return text("select * from pgmq.pop(:queue_name);"), {"queue_name": queue_name}


def delete(queue_name: str, msg_id: int) -> STATEMENT_TYPE:
    """Delete a message from a queue."""
    return text(f"select * from pgmq.delete('{queue_name}',{msg_id}::BIGINT);")


def delete_batch(queue_name: str, msg_ids: List[int]) -> STATEMENT_TYPE:
    """Delete a batch of messages from a queue."""
    # should add explicit type casts to choose the correct candidate function
    return text(f"select * from pgmq.delete('{queue_name}',ARRAY{msg_ids});")


def archive(queue_name: str, msg_id: int) -> STATEMENT_TYPE:
    """Archive a message from a queue."""
    return text(f"select pgmq.archive('{queue_name}',{msg_id}::BIGINT);")


def archive_batch(queue_name: str, msg_ids: List[int]) -> STATEMENT_TYPE:
    """Archive multiple messages from a queue."""
    return text(f"select * from pgmq.archive('{queue_name}',ARRAY{msg_ids});")


def purge(queue_name: str) -> STATEMENT_TYPE:
    """Purge a queue."""
    return text("select pgmq.purge_queue(:queue_name);"), {"queue_name": queue_name}


def metrics(queue_name: str) -> STATEMENT_TYPE:
    """Get metrics for a queue."""
    return text("select * from pgmq.metrics(:queue_name);"), {"queue_name": queue_name}


def metrics_all() -> STATEMENT_TYPE:
    """Get metrics for all queues."""
    return text("select * from pgmq.metrics_all();")
