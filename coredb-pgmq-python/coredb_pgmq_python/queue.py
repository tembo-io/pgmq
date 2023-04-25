from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Union

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

    def create_queue(self, queue: str) -> None:
        """Create a queue"""
        with self.pool.connection() as conn:
            conn.execute("select pgmq_create(%s);", [queue])

    def create_(
        self, queue: str, partition_interval: Optional[str] = "daily", retention_interval: Optional[str] = "5 days"
    ) -> None:
        """Create a partitioned queue"""
        with self.pool.connection() as conn:
            conn.execute("select pgmq_create_non_partitioned(%s, %s);", [queue, partition_interval, retention_interval])

    def send(self, queue: str, message: dict, delay: Optional[int] = None) -> int:
        """Send a message to a queue"""

        with self.pool.connection() as conn:
            if delay is not None:
                # TODO(chuckend): implement send_delay in pgmq
                raise NotImplementedError("send_delay is not implemented in pgmq")
            message = conn.execute(
                "select * from pgmq_send(%s, %s);",
                [queue, Jsonb(message)],  # type: ignore
            ).fetchall()
        return message[0][0]

    def read(self, queue: str, vt: Optional[int] = None, limit: int = 1) -> Union[Message, list[Message]]:
        """Read a message from a queue"""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq_read(%s, %s, %s);", [queue, vt or self.vt, limit]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0] if len(messages) == 1 else messages

    def pop(self, queue: str) -> Message:
        """Read a message from a queue"""
        with self.pool.connection() as conn:
            rows = conn.execute("select * from pgmq_pop(%s);", [queue]).fetchall()

        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0]

    def delete(self, queue: str, msg_id: int) -> bool:
        """Delete a message from a queue"""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq_delete(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]

    def archive(self, queue: str, msg_id: int) -> bool:
        """Archive a message from a queue"""
        with self.pool.connection() as conn:
            row = conn.execute("select pgmq_archive(%s, %s);", [queue, msg_id]).fetchall()

        return row[0][0]
