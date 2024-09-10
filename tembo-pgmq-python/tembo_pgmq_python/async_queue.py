from dataclasses import dataclass, field
from typing import Optional, List
import asyncpg
import os

from orjson import dumps, loads

from tembo_pgmq_python.messages import Message, QueueMetrics


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
    pool: asyncpg.pool.Pool = field(init=False)

    def __post_init__(self) -> None:
        self.host = self.host or "localhost"
        self.port = self.port or "5432"
        self.database = self.database or "postgres"
        self.username = self.username or "postgres"
        self.password = self.password or "postgres"

        if not all([self.host, self.port, self.database, self.username, self.password]):
            raise ValueError("Incomplete database connection information provided.")

    async def init(self):
        self.pool = await asyncpg.create_pool(
            user=self.username,
            database=self.database,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        async with self.pool.acquire() as conn:
            await conn.fetch("create extension if not exists pgmq cascade;")

    async def create_partitioned_queue(
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

        async with self.pool.acquire() as conn:
            await conn.execute(
                "SELECT pgmq.create($1, $2::text, $3::text);",
                queue,
                partition_interval,
                retention_interval,
            )

    async def create_queue(self, queue: str, unlogged: bool = False) -> None:
        """Create a new queue."""
        async with self.pool.acquire() as conn:
            if unlogged:
                await conn.execute("SELECT pgmq.create_unlogged($1);", queue)
            else:
                await conn.execute("SELECT pgmq.create($1);", queue)

    async def validate_queue_name(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT pgmq.validate_queue_name($1);", queue_name)

    async def drop_queue(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue."""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow("SELECT pgmq.drop_queue($1, $2);", queue, partitioned)
        return result[0]

    async def list_queues(self) -> List[str]:
        """List all queues."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT queue_name FROM pgmq.list_queues();")
        return [row["queue_name"] for row in rows]

    async def send(self, queue: str, message: dict, delay: int = 0) -> int:
        """Send a message to a queue."""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send($1, $2::jsonb, $3);", queue, dumps(message).decode("utf-8"), delay
            )
        return result[0]

    async def send_batch(self, queue: str, messages: List[dict], delay: int = 0) -> List[int]:
        """Send a batch of messages to a queue."""
        jsonb_array = [dumps(message).decode("utf-8") for message in messages]

        async with self.pool.acquire() as conn:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3);",
                queue,
                jsonb_array,
                delay,
            )
        return [message[0] for message in result]

    async def read(self, queue: str, vt: Optional[int] = None) -> Optional[Message]:
        """Read a message from a queue."""
        batch_size = 1
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM pgmq.read($1, $2, $3);", queue, vt or self.vt, batch_size)
        messages = [
            Message(msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=loads(row[4])) for row in rows
        ]
        return messages[0] if len(messages) == 1 else None

    async def read_batch(self, queue: str, vt: Optional[int] = None, batch_size=1) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM pgmq.read($1, $2, $3);", queue, vt or self.vt, batch_size)

        return [
            Message(msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=loads(row[4])) for row in rows
        ]

    async def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5);",
                queue,
                vt or self.vt,
                qty,
                max_poll_seconds,
                poll_interval_ms,
            )

        return [
            Message(msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=loads(row[4])) for row in rows
        ]

    async def pop(self, queue: str) -> Message:
        """Pop a message from a queue."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM pgmq.pop($1);", queue)

        messages = [
            Message(msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=loads(row[4])) for row in rows
        ]
        return messages[0]

    async def delete(self, queue: str, msg_id: int) -> bool:
        """Delete a message from a queue."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT pgmq.delete($1::text, $2::int);", queue, msg_id)

        return row[0]

    async def delete_batch(self, queue: str, msg_ids: List[int]) -> List[int]:
        """Delete multiple messages from a queue."""
        async with self.pool.acquire() as conn:
            results = await conn.fetch("SELECT * FROM pgmq.delete($1::text, $2::int[]);", queue, msg_ids)
        return [result[0] for result in results]

    async def archive(self, queue: str, msg_id: int) -> bool:
        """Archive a message from a queue."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT pgmq.archive($1::text, $2::int);", queue, msg_id)

        return row[0]

    async def archive_batch(self, queue: str, msg_ids: List[int]) -> List[int]:
        """Archive multiple messages from a queue."""
        async with self.pool.acquire() as conn:
            results = await conn.fetch("SELECT * FROM pgmq.archive($1::text, $2::int[]);", queue, msg_ids)
        return [result[0] for result in results]

    async def purge(self, queue: str) -> int:
        """Purge a queue."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT pgmq.purge_queue($1);", queue)

        return row[0]

    async def metrics(self, queue: str) -> QueueMetrics:
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow("SELECT * FROM pgmq.metrics($1);", queue)
        return QueueMetrics(
            queue_name=result[0],
            queue_length=result[1],
            newest_msg_age_sec=result[2],
            oldest_msg_age_sec=result[3],
            total_messages=result[4],
            scrape_time=result[5],
        )

    async def metrics_all(self) -> List[QueueMetrics]:
        async with self.pool.acquire() as conn:
            results = await conn.fetch("SELECT * FROM pgmq.metrics_all();")
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

    async def set_vt(self, queue: str, msg_id: int, vt: int) -> Message:
        """Set the visibility timeout for a specific message."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM pgmq.set_vt($1, $2, $3);", queue, msg_id, vt)
        return Message(msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4])

    async def detach_archive(self, queue: str) -> None:
        """Detach an archive from a queue."""
        async with self.pool.acquire() as conn:
            await conn.fetch("select pgmq.detach_archive($1);", queue)
