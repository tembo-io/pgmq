from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Callable, Union
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
import functools
import os


@dataclass
class Message:
    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict


@dataclass
class QueueMetrics:
    queue_name: str
    queue_length: int
    newest_msg_age_sec: int
    oldest_msg_age_sec: int
    total_messages: int
    scrape_time: datetime


def transaction(func: Callable) -> Callable:
    """Decorator to run a method within a database transaction."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        perform_transaction = kwargs.pop(
            "perform_transaction", self.perform_transaction
        )
        if perform_transaction:
            with self.pool.connection() as conn:
                try:
                    with conn.transaction():
                        return func(self, *args, conn=conn, **kwargs)
                except Exception as e:
                    conn.rollback()
                    raise e
        else:
            with self.pool.connection() as conn:
                return func(self, *args, conn=conn, **kwargs)

    return wrapper


@dataclass
class PGMQueue:
    """Base class for interacting with a queue"""

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    kwargs: dict = field(default_factory=dict)
    pool: ConnectionPool = field(init=False)
    perform_transaction: bool = False

    def __post_init__(self) -> None:
        conninfo = f"""
        host={self.host}
        port={self.port}
        dbname={self.database}
        user={self.username}
        password={self.password}
        """
        self.pool = ConnectionPool(conninfo, open=True, **self.kwargs)
        self._initialize_extensions()

    def _initialize_extensions(self) -> None:
        self._execute_query("create extension if not exists pgmq cascade;")

    def _execute_query(
        self, query: str, params: Optional[Union[List, tuple]] = None
    ) -> None:
        with self.pool.connection() as conn:
            conn.execute(query, params)

    def _execute_query_with_result(
        self, query: str, params: Optional[Union[List, tuple]] = None
    ):
        with self.pool.connection() as conn:
            return conn.execute(query, params).fetchall()

    @transaction
    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        """Create a new queue

        Note: Partitions are created pg_partman which must be configured in postgresql.conf
            Set `pg_partman_bgw.interval` to set the interval for partition creation and deletion.
            A value of 10 will create new/delete partitions every 10 seconds. This value should be tuned
            according to the volume of messages being sent to the queue.

        Args:
            queue: The name of the queue.
            partition_interval: The number of messages per partition. Defaults to 10,000.
            retention_interval: The number of messages to retain. Messages exceeding this number will be dropped.
                Defaults to 100,000.
        """
        query = "select pgmq.create(%s, %s::text, %s::text);"
        params = [queue, partition_interval, retention_interval]
        self._execute_query(query, params)

    @transaction
    def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        query = (
            "select pgmq.create_unlogged(%s);"
            if unlogged
            else "select pgmq.create(%s);"
        )
        conn.execute(query, [queue])  # Use the provided connection

    def validate_queue_name(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        query = "select pgmq.validate_queue_name(%s);"
        self._execute_query(query, [queue_name])

    @transaction
    def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        """Drop a queue."""
        query = "select pgmq.drop_queue(%s, %s);"
        result = self._execute_query_with_result(query, [queue, partitioned])
        return result[0][0]

    @transaction
    def list_queues(self, conn=None) -> List[str]:
        """List all queues."""
        query = "select queue_name from pgmq.list_queues();"
        rows = self._execute_query_with_result(query)
        return [row[0] for row in rows]

    @transaction
    def send(self, queue: str, message: dict, delay: int = 0, conn=None) -> int:
        """Send a message to a queue."""
        query = "select * from pgmq.send(%s, %s, %s);"
        result = self._execute_query_with_result(query, [queue, Jsonb(message), delay])
        return result[0][0]

    @transaction
    def send_batch(
        self, queue: str, messages: List[dict], delay: int = 0, conn=None
    ) -> List[int]:
        """Send a batch of messages to a queue."""
        query = "select * from pgmq.send_batch(%s, %s, %s);"
        params = [queue, [Jsonb(message) for message in messages], delay]
        result = self._execute_query_with_result(query, params)
        return [message[0] for message in result]

    @transaction
    def read(
        self, queue: str, vt: Optional[int] = None, conn=None
    ) -> Optional[Message]:
        """Read a message from a queue."""
        query = "select * from pgmq.read(%s, %s, %s);"
        rows = self._execute_query_with_result(query, [queue, vt or self.vt, 1])
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]
        return messages[0] if messages else None

    @transaction
    def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size=1, conn=None
    ) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        query = "select * from pgmq.read(%s, %s, %s);"
        rows = self._execute_query_with_result(
            query, [queue, vt or self.vt, batch_size]
        )
        return [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

    @transaction
    def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        query = "select * from pgmq.read_with_poll(%s, %s, %s, %s, %s);"
        params = [queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms]
        rows = self._execute_query_with_result(query, params)
        return [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

    @transaction
    def pop(self, queue: str, conn=None) -> Message:
        """Pop a message from a queue."""
        query = "select * from pgmq.pop(%s);"
        rows = self._execute_query_with_result(query, [queue])
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]
        return messages[0]

    @transaction
    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a message from a queue."""
        query = "select pgmq.delete(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id])
        return result[0][0]

    @transaction
    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete multiple messages from a queue."""
        query = "select * from pgmq.delete(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_ids])
        return [x[0] for x in result]

    @transaction
    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a message from a queue."""
        query = "select pgmq.archive(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id])
        return result[0][0]

    @transaction
    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive multiple messages from a queue."""
        query = "select * from pgmq.archive(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_ids])
        return [x[0] for x in result]

    @transaction
    def purge(self, queue: str, conn=None) -> int:
        """Purge a queue."""
        query = "select pgmq.purge_queue(%s);"
        result = self._execute_query_with_result(query, [queue])
        return result[0][0]

    @transaction
    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        query = "SELECT * FROM pgmq.metrics(%s);"
        result = self._execute_query_with_result(query, [queue])[0]
        return QueueMetrics(
            queue_name=result[0],
            queue_length=result[1],
            newest_msg_age_sec=result[2],
            oldest_msg_age_sec=result[3],
            total_messages=result[4],
            scrape_time=result[5],
        )

    @transaction
    def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get metrics for all queues."""
        query = "SELECT * FROM pgmq.metrics_all();"
        results = self._execute_query_with_result(query)
        return [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
                scrape_time=row[5],
            )
            for row in results
        ]

    @transaction
    def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        """Set the visibility timeout for a specific message."""
        query = "select * from pgmq.set_vt(%s, %s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id, vt])[0]
        return Message(
            msg_id=result[0],
            read_ct=result[1],
            enqueued_at=result[2],
            vt=result[3],
            message=result[4],
        )

    @transaction
    def detach_archive(self, queue: str, conn=None) -> None:
        """Detach an archive from a queue."""
        query = "select pgmq.detach_archive(%s);"
        self._execute_query(query, [queue])
