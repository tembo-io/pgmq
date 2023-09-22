from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool


@dataclass
class Message:
    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict


@dataclass
class PGMQueue:
    """Base class for interacting with a queue"""

    host: str = "localhost"
    port: str = "5432"
    database: str = "postgres"
    delay: int = 0
    vt: int = 30

    username: str = "postgres"
    password: str = "postgres"

    pool_size: int = 10

    kwargs: dict = field(default_factory=dict)

    pool: ConnectionPool = field(init=False)

    def __post_init__(self) -> None:
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
        self, queue: str, partition_interval: int = 10000, retention_interval: int = 100000
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
            conn.execute("select pgmq.create(%s, %s::text, %s::text);", [queue, partition_interval, retention_interval])

    def create_queue(self, queue: str) -> None:
        """Create a new queue
        Args:
            queue: The name of the queue.
        """

        with self.pool.connection() as conn:
            conn.execute("select pgmq.create(%s);", [queue])

    def send(self, queue: str, message: dict, delay: int = 0) -> int:
        """Send a message to a queue"""

        with self.pool.connection() as conn:
            message = conn.execute(
                "select * from pgmq.send(%s, %s,%s);",
                [queue, Jsonb(message), delay],  # type: ignore
            ).fetchall()
        return message[0][0]

    def send_batch(self, queue: str, messages: list[dict], delay: int = 0) -> list[int]:
        """Send a batch of messages to a queue"""

        with self.pool.connection() as conn:
            result = conn.execute(
                "select * from pgmq.send_batch(%s, %s, %s);",
                [queue, [Jsonb(message) for message in messages], delay],  # type: ignore
            ).fetchall()
        return [message[0] for message in result]

    def read(self, queue: str, vt: Optional[int] = None) -> Optional[Message]:
        """Read a message from a queue"""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq.read(%s, %s, %s);", [queue, vt or self.vt, 1]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0] if len(messages) == 1 else None

    def read_batch(self, queue: str, vt: Optional[int] = None, batch_size=1) -> Optional[list[Message]]:
        """Read a batch of messages from a queue"""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq.read(%s, %s, %s);", [queue, vt or self.vt, batch_size]).fetchall()

        return [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]

    def pop(self, queue: str) -> Message:
        """Read a message from a queue"""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq.pop(%s);", [queue]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0]

    def delete(self, queue: str, msg_id: int) -> bool:
        """Delete a message from a queue"""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq.delete(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]

    def archive(self, queue: str, msg_id: int) -> bool:
        """Archive a message from a queue"""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq.archive(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]
