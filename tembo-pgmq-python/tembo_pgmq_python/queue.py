from dataclasses import dataclass, field
from typing import Optional, List, Union
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
import os
from tembo_pgmq_python.messages import Message, QueueMetrics
from tembo_pgmq_python.decorators import transaction
import logging
import datetime


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
    verbose: bool = False
    log_filename: Optional[str] = None
    pool: ConnectionPool = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        conninfo = f"""
        host={self.host}
        port={self.port}
        dbname={self.database}
        user={self.username}
        password={self.password}
        """
        self.pool = ConnectionPool(conninfo, open=True, **self.kwargs)
        self._initialize_logging()
        self._initialize_extensions()

    def _initialize_logging(self) -> None:
        self.logger = logging.getLogger(__name__)

        if self.verbose:
            log_filename = self.log_filename or datetime.now().strftime("pgmq_debug_%Y%m%d_%H%M%S.log")
            file_handler = logging.FileHandler(filename=os.path.join(os.getcwd(), log_filename))
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.WARNING)

    def _initialize_extensions(self, conn=None) -> None:
        self._execute_query("create extension if not exists pgmq cascade;", conn=conn)

    def _execute_query(self, query: str, params: Optional[Union[List, tuple]] = None, conn=None) -> None:
        self.logger.debug(f"Executing query: {query} with params: {params} using conn: {conn}")
        if conn:
            conn.execute(query, params)
        else:
            with self.pool.connection() as conn:
                conn.execute(query, params)

    def _execute_query_with_result(self, query: str, params: Optional[Union[List, tuple]] = None, conn=None):
        self.logger.debug(f"Executing query with result: {query} with params: {params} using conn: {conn}")
        if conn:
            return conn.execute(query, params).fetchall()
        else:
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
        """Create a new queue"""
        query = "select pgmq.create(%s, %s::text, %s::text);"
        params = [queue, partition_interval, retention_interval]
        self._execute_query(query, params, conn=conn)

    @transaction
    def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        self.logger.debug(f"create_queue called with conn: {conn}")
        query = "select pgmq.create_unlogged(%s);" if unlogged else "select pgmq.create(%s);"
        self._execute_query(query, [queue], conn=conn)

    def validate_queue_name(self, queue_name: str, conn=None) -> None:
        """Validate the length of a queue name."""
        query = "select pgmq.validate_queue_name(%s);"
        self._execute_query(query, [queue_name], conn=conn)

    @transaction
    def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        """Drop a queue."""
        self.logger.debug(f"drop_queue called with conn: {conn}")
        query = "select pgmq.drop_queue(%s, %s);"
        result = self._execute_query_with_result(query, [queue, partitioned], conn=conn)
        return result[0][0]

    @transaction
    def list_queues(self, conn=None) -> List[str]:
        """List all queues."""
        self.logger.debug(f"list_queues called with conn: {conn}")
        query = "select queue_name from pgmq.list_queues();"
        rows = self._execute_query_with_result(query, conn=conn)
        return [row[0] for row in rows]

    @transaction
    def send(self, queue: str, message: dict, delay: int = 0, tz: datetime = None, conn=None) -> int:
        """Send a message to a queue."""
        self.logger.debug(f"send called with conn: {conn}")
        result = None
        if delay:
            query = "select * from pgmq.send(%s::text, %s::jsonb, %s::integer);"
            result = self._execute_query_with_result(query, [queue, Jsonb(message), delay], conn=conn)
        elif tz:
            query = "select * from pgmq.send(%s::text, %s::jsonb, %s::timestamptz);"
            result = self._execute_query_with_result(query, [queue, Jsonb(message), tz], conn=conn)
        else:
            query = "select * from pgmq.send(%s::text, %s::jsonb);"
            result = self._execute_query_with_result(query, [queue, Jsonb(message)], conn=conn)
        return result[0][0]

    @transaction
    def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: datetime = None,
        conn=None,
    ) -> List[int]:
        """Send a batch of messages to a queue."""
        self.logger.debug(f"send_batch called with conn: {conn}")
        result = None
        if delay:
            query = "select * from pgmq.send_batch(%s::text, %s::jsonb[], %s::integer);"
            params = [queue, [Jsonb(message) for message in messages], delay]
            result = self._execute_query_with_result(query, params, conn=conn)
        elif tz:
            query = "select * from pgmq.send_batch(%s::text, %s::jsonb[], %s::timestamptz);"
            params = [queue, [Jsonb(message) for message in messages], tz]
            result = self._execute_query_with_result(query, params, conn=conn)
        else:
            query = "select * from pgmq.send_batch(%s::text, %s::jsonb[]);"
            params = [queue, [Jsonb(message) for message in messages]]
            result = self._execute_query_with_result(query, params, conn=conn)
        return [message[0] for message in result]

    @transaction
    def read(self, queue: str, vt: Optional[int] = None, conn=None) -> Optional[Message]:
        """Read a message from a queue."""
        self.logger.debug(f"read called with conn: {conn}")
        query = "select * from pgmq.read(%s::text, %s::integer, %s::integer);"
        rows = self._execute_query_with_result(query, [queue, vt or self.vt, 1], conn=conn)
        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0] if messages else None

    @transaction
    def read_batch(self, queue: str, vt: Optional[int] = None, batch_size=1, conn=None) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        self.logger.debug(f"read_batch called with conn: {conn}")
        query = "select * from pgmq.read(%s::text, %s::integer, %s::integer);"
        rows = self._execute_query_with_result(query, [queue, vt or self.vt, batch_size], conn=conn)
        return [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]

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
        self.logger.debug(f"read_with_poll called with conn: {conn}")
        query = "select * from pgmq.read_with_poll(%s::text, %s::integer, %s::integer, %s::integer, %s::integer);"
        params = [queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms]
        rows = self._execute_query_with_result(query, params, conn=conn)
        return [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]

    @transaction
    def pop(self, queue: str, conn=None) -> Message:
        """Pop a message from a queue."""
        self.logger.debug(f"pop called with conn: {conn}")
        query = "select * from pgmq.pop(%s);"
        rows = self._execute_query_with_result(query, [queue], conn=conn)
        messages = [Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4]) for x in rows]
        return messages[0]

    @transaction
    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a message from a queue."""
        self.logger.debug(f"delete called with conn: {conn}")
        query = "select pgmq.delete(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id], conn=conn)
        return result[0][0]

    @transaction
    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete multiple messages from a queue."""
        self.logger.debug(f"delete_batch called with conn: {conn}")
        query = "select * from pgmq.delete(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_ids], conn=conn)
        return [x[0] for x in result]

    @transaction
    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a message from a queue."""
        self.logger.debug(f"archive called with conn: {conn}")
        query = "select pgmq.archive(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id], conn=conn)
        return result[0][0]

    @transaction
    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive multiple messages from a queue."""
        self.logger.debug(f"archive_batch called with conn: {conn}")
        query = "select * from pgmq.archive(%s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_ids], conn=conn)
        return [x[0] for x in result]

    @transaction
    def purge(self, queue: str, conn=None) -> int:
        """Purge a queue."""
        self.logger.debug(f"purge called with conn: {conn}")
        query = "select pgmq.purge_queue(%s);"
        result = self._execute_query_with_result(query, [queue], conn=conn)
        return result[0][0]

    @transaction
    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        self.logger.debug(f"metrics called with conn: {conn}")
        query = "SELECT * FROM pgmq.metrics(%s);"
        result = self._execute_query_with_result(query, [queue], conn=conn)[0]
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
        self.logger.debug(f"metrics_all called with conn: {conn}")
        query = "SELECT * FROM pgmq.metrics_all();"
        results = self._execute_query_with_result(query, conn=conn)
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
        self.logger.debug(f"set_vt called with conn: {conn}")
        query = "select * from pgmq.set_vt(%s, %s, %s);"
        result = self._execute_query_with_result(query, [queue, msg_id, vt], conn=conn)[0]
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
        self.logger.debug(f"detach_archive called with conn: {conn}")
        query = "select pgmq.detach_archive(%s);"
        self._execute_query(query, [queue], conn=conn)
