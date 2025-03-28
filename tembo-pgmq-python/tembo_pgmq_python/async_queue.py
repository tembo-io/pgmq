# async_queue.py

from dataclasses import dataclass, field
from typing import Optional, List
import asyncpg
import os
import logging
from datetime import datetime

from orjson import dumps, loads

from tembo_pgmq_python.messages import Message, QueueMetrics
from tembo_pgmq_python.decorators import async_transaction as transaction


@dataclass
class PGMQueue:
    """Asynchronous PGMQueue client for interacting with queues."""

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    perform_transaction: bool = False
    verbose: bool = False
    log_filename: Optional[str] = None
    pool: asyncpg.pool.Pool = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.host = self.host or "localhost"
        self.port = self.port or "5432"
        self.database = self.database or "postgres"
        self.username = self.username or "postgres"
        self.password = self.password or "postgres"

        if not all([self.host, self.port, self.database, self.username, self.password]):
            raise ValueError("Incomplete database connection information provided.")

        self._initialize_logging()
        self.logger.debug("PGMQueue initialized")

    def _initialize_logging(self) -> None:
        self.logger = logging.getLogger(__name__)

        if self.verbose:
            log_filename = self.log_filename or datetime.now().strftime("pgmq_async_debug_%Y%m%d_%H%M%S.log")
            file_handler = logging.FileHandler(filename=os.path.join(os.getcwd(), log_filename))
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.WARNING)

    async def init(self):
        self.logger.debug("Creating asyncpg connection pool")
        self.pool = await asyncpg.create_pool(
            user=self.username,
            database=self.database,
            password=self.password,
            host=self.host,
            port=self.port,
            min_size=1,
            max_size=self.pool_size,
        )
        self.logger.debug("Initializing pgmq extension")
        async with self.pool.acquire() as conn:
            await conn.execute("create extension if not exists pgmq cascade;")

    @transaction
    async def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        """Create a new partitioned queue."""
        self.logger.debug(
            f"create_partitioned_queue called with queue='{queue}', "
            f"partition_interval={partition_interval}, "
            f"retention_interval={retention_interval}, conn={conn}"
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                await self._create_partitioned_queue_internal(queue, partition_interval, retention_interval, conn)
        else:
            await self._create_partitioned_queue_internal(queue, partition_interval, retention_interval, conn)

    async def _create_partitioned_queue_internal(self, queue, partition_interval, retention_interval, conn):
        self.logger.debug(f"Creating partitioned queue '{queue}'")
        await conn.execute(
            "SELECT pgmq.create($1, $2::text, $3::text);",
            queue,
            partition_interval,
            retention_interval,
        )

    @transaction
    async def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        self.logger.debug(f"create_queue called with queue='{queue}', unlogged={unlogged}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                await self._create_queue_internal(queue, unlogged, conn)
        else:
            await self._create_queue_internal(queue, unlogged, conn)

    async def _create_queue_internal(self, queue, unlogged, conn):
        self.logger.debug(f"Creating queue '{queue}' with unlogged={unlogged}")
        if unlogged:
            await conn.execute("SELECT pgmq.create_unlogged($1);", queue)
        else:
            await conn.execute("SELECT pgmq.create($1);", queue)

    async def validate_queue_name(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        self.logger.debug(f"validate_queue_name called with queue_name='{queue_name}'")
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT pgmq.validate_queue_name($1);", queue_name)

    @transaction
    async def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        """Drop a queue."""
        self.logger.debug(f"drop_queue called with queue='{queue}', partitioned={partitioned}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._drop_queue_internal(queue, partitioned, conn)
        else:
            return await self._drop_queue_internal(queue, partitioned, conn)

    async def _drop_queue_internal(self, queue, partitioned, conn):
        result = await conn.fetchrow("SELECT pgmq.drop_queue($1, $2);", queue, partitioned)
        self.logger.debug(f"Queue '{queue}' dropped: {result[0]}")
        return result[0]

    @transaction
    async def list_queues(self, conn=None) -> List[str]:
        """List all queues."""
        self.logger.debug(f"list_queues called with conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._list_queues_internal(conn)
        else:
            return await self._list_queues_internal(conn)

    async def _list_queues_internal(self, conn):
        rows = await conn.fetch("SELECT queue_name FROM pgmq.list_queues();")
        queues = [row["queue_name"] for row in rows]
        self.logger.debug(f"Queues listed: {queues}")
        return queues

    @transaction
    async def send(self, queue: str, message: dict, delay: int = 0, tz: datetime = None, conn=None) -> int:
        """Send a message to a queue."""
        self.logger.debug(f"send called with queue='{queue}', message={message}, delay={delay}, tz={tz}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._send_internal(queue, message, delay, tz, conn)
        else:
            return await self._send_internal(queue, message, delay, tz, conn)

    async def _send_internal(
        self,
        queue: str,
        message: dict,
        delay: int = None,
        tz: datetime = None,
        conn=None,
    ):
        self.logger.debug(f"Sending message to queue '{queue}' with delay={delay}, tz={tz}")
        result = None
        if delay:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer);",
                queue,
                dumps(message).decode("utf-8"),
                delay,
            )
        elif tz:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::timestamptz);",
                queue,
                dumps(message).decode("utf-8"),
                tz,
            )
        else:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send($1::text, $2::jsonb);",
                queue,
                dumps(message).decode("utf-8"),
            )
        self.logger.debug(f"Message sent with msg_id={result[0]}")
        return result[0]

    @transaction
    async def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: str = None,
        conn=None,
    ) -> List[int]:
        """Send a batch of messages to a queue."""
        self.logger.debug(
            f"send_batch called with queue='{queue}', messages={messages}, delay={delay}, tz={tz}, conn={conn}"
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._send_batch_internal(queue, messages, delay, tz, conn)
        else:
            return await self._send_batch_internal(queue, messages, delay, tz, conn)

    async def _send_batch_internal(
        self,
        queue: str,
        messages: list[dict],
        delay: int = None,
        tz: datetime = None,
        conn=None,
    ):
        self.logger.debug(f"Sending batch of messages to queue '{queue}' with delay={delay}, tz={tz}")
        jsonb_array = [dumps(message).decode("utf-8") for message in messages]
        result = None
        if delay:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::integer);",
                queue,
                jsonb_array,
                delay,
            )
        elif tz:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::integer);",
                queue,
                jsonb_array,
                tz,
            )
        else:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch($1, $2::jsonb[]);",
                queue,
                jsonb_array,
            )
        msg_ids = [message[0] for message in result]
        self.logger.debug(f"Batch messages sent with msg_ids={msg_ids}")
        return msg_ids

    @transaction
    async def read(self, queue: str, vt: Optional[int] = None, conn=None) -> Optional[Message]:
        """Read a message from a queue."""
        self.logger.debug(f"read called with queue='{queue}', vt={vt}, conn={conn}")
        batch_size = 1
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_internal(queue, vt, batch_size, conn)
        else:
            return await self._read_internal(queue, vt, batch_size, conn)

    async def _read_internal(self, queue, vt, batch_size, conn):
        self.logger.debug(f"Reading message from queue '{queue}' with vt={vt}")
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer);",
            queue,
            vt or self.vt,
            batch_size,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
        ]
        self.logger.debug(f"Message read: {messages[0] if messages else None}")
        return messages[0] if messages else None

    @transaction
    async def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size=1, conn=None
    ) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        self.logger.debug(f"read_batch called with queue='{queue}', vt={vt}, batch_size={batch_size}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_batch_internal(queue, vt, batch_size, conn)
        else:
            return await self._read_batch_internal(queue, vt, batch_size, conn)

    async def _read_batch_internal(self, queue, vt, batch_size, conn):
        self.logger.debug(f"Reading batch of messages from queue '{queue}' with vt={vt}")
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer);",
            queue,
            vt or self.vt,
            batch_size,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
        ]
        self.logger.debug(f"Batch messages read: {messages}")
        return messages

    @transaction
    async def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        self.logger.debug(
            f"read_with_poll called with queue='{queue}', vt={vt}, qty={qty}, "
            f"max_poll_seconds={max_poll_seconds}, poll_interval_ms={poll_interval_ms}, conn={conn}"
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_with_poll_internal(queue, vt, qty, max_poll_seconds, poll_interval_ms, conn)
        else:
            return await self._read_with_poll_internal(queue, vt, qty, max_poll_seconds, poll_interval_ms, conn)

    async def _read_with_poll_internal(self, queue, vt, qty, max_poll_seconds, poll_interval_ms, conn):
        self.logger.debug(f"Reading messages with polling from queue '{queue}'")
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5);",
            queue,
            vt or self.vt,
            qty,
            max_poll_seconds,
            poll_interval_ms,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
        ]
        self.logger.debug(f"Messages read with polling: {messages}")
        return messages

    @transaction
    async def pop(self, queue: str, conn=None) -> Message:
        """Pop a message from a queue."""
        self.logger.debug(f"pop called with queue='{queue}', conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._pop_internal(queue, conn)
        else:
            return await self._pop_internal(queue, conn)

    async def _pop_internal(self, queue, conn):
        self.logger.debug(f"Popping message from queue '{queue}'")
        rows = await conn.fetch("SELECT * FROM pgmq.pop($1);", queue)
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
        ]
        self.logger.debug(f"Message popped: {messages[0] if messages else None}")
        return messages[0] if messages else None

    @transaction
    async def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a message from a queue."""
        self.logger.debug(f"delete called with queue='{queue}', msg_id={msg_id}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._delete_internal(queue, msg_id, conn)
        else:
            return await self._delete_internal(queue, msg_id, conn)

    async def _delete_internal(self, queue, msg_id, conn):
        self.logger.debug(f"Deleting message with msg_id={msg_id} from queue '{queue}'")
        row = await conn.fetchrow("SELECT pgmq.delete($1::text, $2::int);", queue, msg_id)
        self.logger.debug(f"Message deleted: {row[0]}")
        return row[0]

    @transaction
    async def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete multiple messages from a queue."""
        self.logger.debug(f"delete_batch called with queue='{queue}', msg_ids={msg_ids}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._delete_batch_internal(queue, msg_ids, conn)
        else:
            return await self._delete_batch_internal(queue, msg_ids, conn)

    async def _delete_batch_internal(self, queue, msg_ids, conn):
        self.logger.debug(f"Deleting messages with msg_ids={msg_ids} from queue '{queue}'")
        results = await conn.fetch("SELECT * FROM pgmq.delete($1::text, $2::int[]);", queue, msg_ids)
        deleted_ids = [result[0] for result in results]
        self.logger.debug(f"Messages deleted: {deleted_ids}")
        return deleted_ids

    @transaction
    async def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a message from a queue."""
        self.logger.debug(f"archive called with queue='{queue}', msg_id={msg_id}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._archive_internal(queue, msg_id, conn)
        else:
            return await self._archive_internal(queue, msg_id, conn)

    async def _archive_internal(self, queue, msg_id, conn):
        self.logger.debug(f"Archiving message with msg_id={msg_id} from queue '{queue}'")
        row = await conn.fetchrow("SELECT pgmq.archive($1::text, $2::int);", queue, msg_id)
        self.logger.debug(f"Message archived: {row[0]}")
        return row[0]

    @transaction
    async def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive multiple messages from a queue."""
        self.logger.debug(f"archive_batch called with queue='{queue}', msg_ids={msg_ids}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._archive_batch_internal(queue, msg_ids, conn)
        else:
            return await self._archive_batch_internal(queue, msg_ids, conn)

    async def _archive_batch_internal(self, queue, msg_ids, conn):
        self.logger.debug(f"Archiving messages with msg_ids={msg_ids} from queue '{queue}'")
        results = await conn.fetch("SELECT * FROM pgmq.archive($1::text, $2::int[]);", queue, msg_ids)
        archived_ids = [result[0] for result in results]
        self.logger.debug(f"Messages archived: {archived_ids}")
        return archived_ids

    @transaction
    async def purge(self, queue: str, conn=None) -> int:
        """Purge a queue."""
        self.logger.debug(f"purge called with queue='{queue}', conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._purge_internal(queue, conn)
        else:
            return await self._purge_internal(queue, conn)

    async def _purge_internal(self, queue, conn):
        self.logger.debug(f"Purging queue '{queue}'")
        row = await conn.fetchrow("SELECT pgmq.purge_queue($1);", queue)
        self.logger.debug(f"Messages purged: {row[0]}")
        return row[0]

    @transaction
    async def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        self.logger.debug(f"metrics called with queue='{queue}', conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._metrics_internal(queue, conn)
        else:
            return await self._metrics_internal(queue, conn)

    async def _metrics_internal(self, queue, conn):
        self.logger.debug(f"Fetching metrics for queue '{queue}'")
        result = await conn.fetchrow("SELECT * FROM pgmq.metrics($1);", queue)
        metrics = QueueMetrics(
            queue_name=result[0],
            queue_length=result[1],
            newest_msg_age_sec=result[2],
            oldest_msg_age_sec=result[3],
            total_messages=result[4],
            scrape_time=result[5],
        )
        self.logger.debug(f"Metrics fetched: {metrics}")
        return metrics

    @transaction
    async def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get metrics for all queues."""
        self.logger.debug(f"metrics_all called with conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._metrics_all_internal(conn)
        else:
            return await self._metrics_all_internal(conn)

    async def _metrics_all_internal(self, conn):
        self.logger.debug("Fetching metrics for all queues")
        results = await conn.fetch("SELECT * FROM pgmq.metrics_all();")
        metrics_list = [
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
        self.logger.debug(f"All metrics fetched: {metrics_list}")
        return metrics_list

    @transaction
    async def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        """Set the visibility timeout for a specific message."""
        self.logger.debug(f"set_vt called with queue='{queue}', msg_id={msg_id}, vt={vt}, conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._set_vt_internal(queue, msg_id, vt, conn)
        else:
            return await self._set_vt_internal(queue, msg_id, vt, conn)

    async def _set_vt_internal(self, queue, msg_id, vt, conn):
        self.logger.debug(f"Setting VT for msg_id={msg_id} in queue '{queue}' to vt={vt}")
        row = await conn.fetchrow("SELECT * FROM pgmq.set_vt($1, $2, $3);", queue, msg_id, vt)
        message = Message(
            msg_id=row[0],
            read_ct=row[1],
            enqueued_at=row[2],
            vt=row[3],
            message=loads(row[4]),
        )
        self.logger.debug(f"VT set for message: {message}")
        return message

    @transaction
    async def detach_archive(self, queue: str, conn=None) -> None:
        """Detach an archive from a queue."""
        self.logger.debug(f"detach_archive called with queue='{queue}', conn={conn}")
        if conn is None:
            async with self.pool.acquire() as conn:
                await self._detach_archive_internal(queue, conn)
        else:
            await self._detach_archive_internal(queue, conn)

    async def _detach_archive_internal(self, queue, conn):
        self.logger.debug(f"Detaching archive from queue '{queue}'")
        await conn.execute("SELECT pgmq.detach_archive($1);", queue)
        self.logger.debug(f"Archive detached from queue '{queue}'")
