from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
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

    def __post_init__(self) -> None:
        self.host = self.host or "localhost"
        self.port = self.port or "5432"
        self.database = self.database or "postgres"
        self.username = self.username or "postgres"
        self.password = self.password or "postgres"

        if not all([self.host, self.port, self.database, self.username, self.password]):
            raise ValueError("Incomplete database connection information provided.")

        conninfo = f"""
        host={self.host}
        port={self.port}
        dbname={self.database}
        user={self.username}
        password={self.password}
        """
        self.pool = ConnectionPool(conninfo, **self.kwargs)

        with self.pool.connection() as conn:
            conn.execute("create extension if not exists pgmq cascade;")

    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
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

        with self.pool.connection() as conn:
            conn.execute(
                "select pgmq.create(%s, %s::text, %s::text);",
                [queue, partition_interval, retention_interval],
            )

    def create_queue(self, queue: str, unlogged: bool = False) -> None:
        """Create a new queue."""
        with self.pool.connection() as conn:
            if unlogged:
                conn.execute("select pgmq.create_unlogged(%s);", [queue])
            else:
                conn.execute("select pgmq.create(%s);", [queue])

    def send(self, queue: str, message: dict, delay: int = 0) -> int:
        """Send a message to a queue."""
        with self.pool.connection() as conn:
            result = conn.execute("select * from pgmq.send(%s, %s, %s);", [queue, Jsonb(message), delay]).fetchall()
        return result[0][0]

    def send_batch(self, queue: str, messages: List[dict], delay: int = 0) -> List[int]:
        """Send a batch of messages to a queue."""
        with self.pool.connection() as conn:
            result = conn.execute(
                "select * from pgmq.send_batch(%s, %s, %s);",
                [queue, [Jsonb(message) for message in messages], delay],
            ).fetchall()
        return [message[0] for message in result]

    def read(self, queue: str, vt: Optional[int] = None) -> Optional[Message]:
        """Read a message from a queue."""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq.read(%s, %s, %s);", [queue, vt or self.vt, 1]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0] if len(messages) == 1 else None

    def read_batch(self, queue: str, vt: Optional[int] = None, batch_size=1) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        with self.pool.connection() as conn:
            rows = conn.execute(
                "select * from pgmq.read(%s, %s, %s);",
                [queue, vt or self.vt, batch_size],
            ).fetchall()

        return [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]

    def pop(self, queue: str) -> Message:
        """Pop a message from a queue."""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq.pop(%s);", [queue]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0]

    def delete(self, queue: str, msg_id: int) -> bool:
        """Delete a message from a queue."""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq.delete(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]

    def archive(self, queue: str, msg_id: int) -> bool:
        """Archive a message from a queue."""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq.archive(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]

    def purge(self, queue: str) -> int:
        """Purge a queue."""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq.purge_queue(%s);", [queue]).fetchall()

        return row[0][0]

    def metrics(self, queue: str) -> QueueMetrics:
        with self.pool.connection() as conn:
            result = conn.execute("SELECT * FROM pgmq.metrics(%s);", [queue]).fetchone()
        return QueueMetrics(
            queue_name=result[0],
            queue_length=result[1],
            newest_msg_age_sec=result[2],
            oldest_msg_age_sec=result[3],
            total_messages=result[4],
            scrape_time=result[5],
        )

    def metrics_all(self) -> List[QueueMetrics]:
        with self.pool.connection() as conn:
            results = conn.execute("SELECT * FROM pgmq.metrics_all();").fetchall()
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
