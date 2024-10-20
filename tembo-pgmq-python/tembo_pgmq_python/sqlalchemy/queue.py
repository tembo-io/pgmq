import asyncio
from typing import List, Optional
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from tembo_pgmq_python.messages import Message, QueueMetrics
from tembo_pgmq_python.sqlalchemy._types import ENGINE_TYPE, DIALECTS_TYPE
from tembo_pgmq_python.sqlalchemy._decorators import inject_session, inject_async_session
from tembo_pgmq_python.sqlalchemy._utils import (
    get_session_type,
    is_async_session_maker,
    is_async_dsn,
    encode_dict_to_psql,
    encode_list_to_psql,
)



class PGMQueue:
    engine: ENGINE_TYPE = None
    session_maker: sessionmaker = None
    delay: int = 0
    vt: int = 30

    is_async: bool = False
    is_pg_partman_ext_checked: bool = False
    loop: asyncio.AbstractEventLoop = None

    def __init__(
        self,
        # for sqlalchemy
        dsn: Optional[str] = None,
        engine: Optional[ENGINE_TYPE] = None,
        session_maker: Optional[sessionmaker] = None,
        # for specifying the connection directly
        dialect: Optional[DIALECTS_TYPE] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        # for logging
        verbose: bool = False,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """

        | There are **4** ways to initialize ``PGMQueue`` class:
        | 1. Initialize with a ``dsn``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue

            pgmq_client = PGMQueue(dsn='postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            # or async dsn
            async_pgmq_client = PGMQueue(dsn='postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')

        | 2. Initialize with an ``engine`` or ``async_engine``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue
            from sqlalchemy import create_engine
            from sqlalchemy.ext.asyncio import create_async_engine

            engine = create_engine('postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            pgmq_client = PGMQueue(engine=engine)
            # or async engine
            async_engine = create_async_engine('postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')
            async_pgmq_client = PGMQueue(engine=async_engine)

        | 3. Initialize with a ``session_maker``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue
            from sqlalchemy.orm import sessionmaker
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

            engine = create_engine('postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            session_maker = sessionmaker(bind=engine)
            pgmq_client = PGMQueue(session_maker=session_maker)
            # or async session_maker
            async_engine = create_async_engine('postgresql+asyncpg://postgres:postgres@localhost:5432/post
            async_session_maker = sessionmaker(bind=async_engine, class_=AsyncSession)
            async_pgmq_client = PGMQueue(session_maker=async_session_maker)

        | 4. Initialize by specifying the connection directly:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue
            
            pgmq_client = PGMQueue(
                dialect='psycopg',
                host='localhost',
                port=5432,
                user='postgres',
                password='postgres',
                database='postgres'
            )
            # ...

        .. note::
            | ``PGMQueue`` will **auto create** the ``pgmq`` extension ( and ``pg_partman`` extension if the method is related with **partitioned_queue** ) if it does not exist in the Postgres.
            | But you must make sure that the ``pgmq`` extension ( or ``pg_partman`` extension ) already **installed** in the Postgres.
        """
        self._initialize_sqlalchemy(dsn, engine, session_maker, dialect, host, port, user, password, database)
        self._initialize_logging(verbose, logger)
        # create pgmq extension if not exists
        self._check_pgmq_ext()

    def _initialize_sqlalchemy(self, dsn: str, engine: ENGINE_TYPE, session_maker: sessionmaker,dialect: DIALECTS_TYPE, host: str, port: int, user: str, password: str, database: str) -> None:
        if not dsn and not engine and not session_maker:
            # check if the connection is specified directly
            if not all([dialect, host, port, user, password, database]):
                raise ValueError(
                    "Must provide either dsn, engine, or session_maker or specify the connection directly"
                )
            dsn = f"postgresql+{dialect}://{user}:{password}@{host}:{port}/{database}"
            
        # initialize the engine and session_maker
        if session_maker:
            self.session_maker = session_maker
            self.is_async = is_async_session_maker(session_maker)
        elif engine:
            self.engine = engine
            self.is_async = self.engine.dialect.is_async
            self.session_maker = sessionmaker(
                bind=self.engine, class_=get_session_type(self.engine)
            )
        else:
            self.engine = (
                create_async_engine(dsn) if is_async_dsn(dsn) else create_engine(dsn)
            )
            self.is_async = self.engine.dialect.is_async
            self.session_maker = sessionmaker(
                bind=self.engine, class_=get_session_type(self.engine)
            )

        if self.is_async:
            self.loop = asyncio.new_event_loop()

    def _initialize_logging(self, verbose: bool, logger: Optional[logging.Logger]) -> None:
        """Initialize the logger."""
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)

        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    async def _check_pgmq_ext_async(self) -> None:
        """Check if the pgmq extension exists."""
        async with self.session_maker() as session:
            await session.execute(text("create extension if not exists pgmq cascade;"))
            await session.commit()

    def _check_pgmq_ext_sync(self) -> None:
        """Check if the pgmq extension exists."""
        with self.session_maker() as session:
            session.execute(text("create extension if not exists pgmq cascade;"))
            session.commit()

    def _check_pgmq_ext(self) -> None:
        """Check if the pgmq extension exists."""
        if self.is_async:
            return self.loop.run_until_complete(self._check_pgmq_ext_async())
        return self._check_pgmq_ext_sync()

    async def _check_pg_partman_ext_async(self) -> None:
        """Check if the pg_partman extension exists."""
        async with self.session_maker() as session:
            await session.execute(
                text("create extension if not exists pg_partman cascade;")
            )
            await session.commit()

    def _check_pg_partman_ext_sync(self) -> None:
        """Check if the pg_partman extension exists."""
        with self.session_maker() as session:
            session.execute(text("create extension if not exists pg_partman cascade;"))
            session.commit()

    def _check_pg_partman_ext(self) -> None:
        """Check if the pg_partman extension exists."""
        if self.is_pg_partman_ext_checked:
            return
        self.is_pg_partman_ext_checked

        if self.is_async:
            return self.loop.run_until_complete(self._check_pg_partman_ext_async())
        return self._check_pg_partman_ext_sync()

    def _create_queue_sync(self, queue_name: str, unlogged: bool = False) -> None:
        """ """
        with self.session_maker() as session:
            if unlogged:
                session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue_name}
                )
            else:
                session.execute(
                    text("select pgmq.create(:queue);"), {"queue": queue_name}
                )
            session.commit()

    async def _create_queue_async(
        self, queue_name: str, unlogged: bool = False
    ) -> None:
        """Create a new queue."""
        async with self.session_maker() as session:
            if unlogged:
                await session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue_name}
                )
            else:
                await session.execute(
                    text("select pgmq.create(:queue);"), {"queue": queue_name}
                )
            await session.commit()

    def create_queue(self, queue_name: str, unlogged: bool = False) -> None:
        """
        .. _unlogged_table: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED
        .. |unlogged_table| replace:: **UNLOGGED TABLE**

        **Create a new queue.**

        * if ``unlogged`` is ``True``, the queue will be created as an |unlogged_table|_ .
        * ``queue_name`` must be **less than 48 characters**.

            .. code-block:: python

                pgmq_client.create_queue('my_queue')
                # or unlogged table queue
                pgmq_client.create_queue('my_queue', unlogged=True)

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._create_queue_async(queue_name, unlogged)
            )
        return self._create_queue_sync(queue_name, unlogged)

    def _create_partitioned_queue_sync(
        self,
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
    ) -> None:
        """Create a new partitioned queue."""
        with self.session_maker() as session:
            session.execute(
                text(
                    "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
                ),
                {
                    "queue_name": queue_name,
                    "partition_interval": partition_interval,
                    "retention_interval": retention_interval,
                },
            )
            session.commit()

    async def _create_partitioned_queue_async(
        self,
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
    ) -> None:
        """Create a new partitioned queue."""
        async with self.session_maker() as session:
            await session.execute(
                text(
                    "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
                ),
                {
                    "queue_name": queue_name,
                    "partition_interval": partition_interval,
                    "retention_interval": retention_interval,
                },
            )
            await session.commit()

    def create_partitioned_queue(
        self,
        queue_name: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
    ) -> None:
        """Create a new **partitioned** queue.

        .. _pgmq_partitioned_queue: https://github.com/tembo-io/pgmq?tab=readme-ov-file#partitioned-queues
        .. |pgmq_partitioned_queue| replace:: **PGMQ: Partitioned Queues**

        .. code-block:: python

                pgmq_client.create_partitioned_queue('my_partitioned_queue', partition_interval=10000, retention_interval=100000)

        Args:
            queue_name (str): The name of the queue, should be less than 48 characters.
            partition_interval (int): Will create a new partition every ``partition_interval`` messages.
            retention_interval (int): The interval for retaining partitions. Any messages that have a `msg_id` less than ``max(msg_id)`` - ``retention_interval`` will be dropped.

                .. note::
                    | Currently, only support for partitioning by **msg_id**.
                    | Will add **time-based partitioning** in the future ``pgmq-sqlalchemy`` release.

        .. important::
            | You must make sure that the ``pg_partman`` extension already **installed** in the Postgres.
            | ``pgmq-sqlalchemy`` will **auto create** the ``pg_partman`` extension if it does not exist in the Postgres.
            | For more details about ``pgmq`` with ``pg_partman``, checkout the |pgmq_partitioned_queue|_.


        """
        # check if the pg_partman extension exists before creating a partitioned queue at runtime
        self._check_pg_partman_ext()

        if self.is_async:
            return self.loop.run_until_complete(
                self._create_partitioned_queue_async(
                    queue_name, str(partition_interval), str(retention_interval)
                )
            )
        return self._create_partitioned_queue_sync(
            queue_name, str(partition_interval), str(retention_interval)
        )

    def _validate_queue_name_sync(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        with self.session_maker() as session:
            session.execute(
                text("select pgmq.validate_queue_name(:queue);"), {"queue": queue_name}
            )
            session.commit()

    async def _validate_queue_name_async(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        async with self.session_maker() as session:
            await session.execute(
                text("select pgmq.validate_queue_name(:queue);"), {"queue": queue_name}
            )
            await session.commit()

    def validate_queue_name(self, queue_name: str) -> None:
        """
        * Will raise an error if the ``queue_name`` is more than 48 characters.
        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._validate_queue_name_async(queue_name)
            )
        return self._validate_queue_name_sync(queue_name)

    def _drop_queue_sync(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue."""
        with self.session_maker() as session:
            row = session.execute(
                text("select pgmq.drop_queue(:queue, :partitioned);"),
                {"queue": queue, "partitioned": partitioned},
            ).fetchone()
            session.commit()
            return row[0]

    async def _drop_queue_async(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select pgmq.drop_queue(:queue, :partitioned);"),
                    {"queue": queue, "partitioned": partitioned},
                )
            ).fetchone()
            await session.commit()
            return row[0]

    def drop_queue(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue.

        .. _drop_queue_method: ref:`pgmq_sqlalchemy.PGMQueue.drop_queue`
        .. |drop_queue_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.drop_queue`

        .. code-block:: python

            pgmq_client.drop_queue('my_queue')
            # for partitioned queue
            pgmq_client.drop_queue('my_partitioned_queue', partitioned=True)

        .. warning::
            | All messages and queue itself will be deleted. (``pgmq.q_<queue_name>`` table)
            | **Archived tables** (``pgmq.a_<queue_name>`` table **will be dropped as well. )**
            |
            | See |archive_method|_ for more details.
        """
        # check if the pg_partman extension exists before dropping a partitioned queue at runtime
        if partitioned:
            self._check_pg_partman_ext()

        if self.is_async:
            return self.loop.run_until_complete(
                self._drop_queue_async(queue, partitioned)
            )
        return self._drop_queue_sync(queue, partitioned)

    def _list_queues_sync(self) -> List[str]:
        """List all queues."""
        with self.session_maker() as session:
            rows = session.execute(
                text("select queue_name from pgmq.list_queues();")
            ).fetchall()
            session.commit()
            return [row[0] for row in rows]

    async def _list_queues_async(self) -> List[str]:
        """List all queues."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text("select queue_name from pgmq.list_queues();")
                )
            ).fetchall()
            await session.commit()
            return [row[0] for row in rows]

    def list_queues(self) -> List[str]:
        """List all queues.

        .. code-block:: python

            queue_list = pgmq_client.list_queues()
            print(queue_list)
        """
        if self.is_async:
            return self.loop.run_until_complete(self._list_queues_async())
        return self._list_queues_sync()

    def _send_sync(self, queue_name: str, message: str, delay: int = 0) -> int:
        with self.session_maker() as session:
            row = (
                session.execute(
                    text(f"select * from pgmq.send('{queue_name}',{message},{delay});")
                )
            ).fetchone()
            session.commit()
        return row[0]

    async def _send_async(self, queue_name: str, message: str, delay: int = 0) -> int:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text(f"select * from pgmq.send('{queue_name}',{message},{delay});")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def send(self, queue_name: str, message: dict, delay: int = 0) -> int:
        """Send a message to a queue.

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value', 'key2': 'value2'})
            print(msg_id)

        Example with delay:

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value', 'key2': 'value2'}, delay=10)
            msg = pgmq_client.read('my_queue')
            assert msg is None
            time.sleep(10)
            msg = pgmq_client.read('my_queue')
            assert msg is not None
        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._send_async(queue_name, encode_dict_to_psql(message), delay)
            )
        return self._send_sync(queue_name, encode_dict_to_psql(message), delay)

    def _send_batch_sync(
        self, queue_name: str, messages: str, delay: int = 0
    ) -> List[int]:
        with self.session_maker() as session:
            rows = (
                session.execute(
                    text(
                        f"select * from pgmq.send_batch('{queue_name}',{messages},{delay});"
                    )
                )
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _send_batch_async(
        self, queue_name: str, messages: str, delay: int = 0
    ) -> List[int]:
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(
                        f"select * from pgmq.send_batch('{queue_name}',{messages},{delay});"
                    )
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def send_batch(
        self, queue_name: str, messages: List[dict], delay: int = 0
    ) -> List[int]:
        """
        Send a batch of messages to a queue.

        .. code-block:: python

            msgs = [{'key': 'value', 'key2': 'value2'}, {'key': 'value', 'key2': 'value2'}]
            msg_ids = pgmq_client.send_batch('my_queue', msgs)
            print(msg_ids)
            # send with delay
            msg_ids = pgmq_client.send_batch('my_queue', msgs, delay=10)

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._send_batch_async(queue_name, encode_list_to_psql(messages), delay)
            )
        return self._send_batch_sync(queue_name, encode_list_to_psql(messages), delay)

    def _read_sync(self, queue_name: str, vt: int) -> Optional[Message]:
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,1);"),
                {"queue_name": queue_name, "vt": vt},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _read_async(self, queue_name: str, vt: int) -> Optional[Message]:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,1);"),
                    {"queue_name": queue_name, "vt": vt},
                )
            ).fetchone()
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def read(self, queue_name: str, vt: Optional[int] = None) -> Optional[Message]:
        """
        .. _for_update_skip_locked: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
        .. |for_update_skip_locked| replace:: **FOR UPDATE SKIP LOCKED**

        .. _read_method: ref:`pgmq_sqlalchemy.PGMQueue.read`
        .. |read_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read`

        Read a message from the queue.

        Returns:
            |schema_message_class|_ or ``None`` if the queue is empty.

        .. note::
            | ``PGMQ`` use |for_update_skip_locked|_ lock to make sure **a message is only read by one consumer**.
            | See the `pgmq.read <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L44-L75>`_ function for more details.
            |
            | For **consumer retries mechanism** (e.g. mark a message as failed after a certain number of retries) can be implemented by using the ``read_ct`` field in the |schema_message_class|_ object.


        .. important::
            | ``vt`` is the **visibility timeout** in seconds.
            | When a message is read from the queue, it will be invisible to other consumers for the duration of the ``vt``.

        Usage:

        .. code-block:: python

            from pgmq_sqlalchemy.schema import Message

            msg:Message = pgmq_client.read('my_queue')
            print(msg.msg_id)
            print(msg.message)
            print(msg.read_ct) # read count, how many times the message has been read

        Example with ``vt``:

        .. code-block:: python

            # assert `read_vt_demo` is empty
            pgmq_client.send('read_vt_demo', {'key': 'value', 'key2': 'value2'})
            msg = pgmq_client.read('read_vt_demo', vt=10)
            assert msg is not None

            # try to read immediately
            msg = pgmq_client.read('read_vt_demo')
            assert msg is None # will return None because the message is still invisible

            # try to read after 5 seconds
            time.sleep(5)
            msg = pgmq_client.read('read_vt_demo')
            assert msg is None # still invisible after 5 seconds

             # try to read after 11 seconds
            time.sleep(6)
            msg = pgmq_client.read('read_vt_demo')
            assert msg is not None # the message is visible after 10 seconds


        """
        if self.is_async:
            return self.loop.run_until_complete(self._read_async(queue_name, vt))
        return self._read_sync(queue_name, vt)

    def _read_batch_sync(
        self,
        queue_name: str,
        vt: int,
        batch_size: int = 1,
    ) -> Optional[List[Message]]:
        if vt is None:
            vt = self.vt
        with self.session_maker() as session:
            rows = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                {
                    "queue_name": queue_name,
                    "vt": vt,
                    "batch_size": batch_size,
                },
            ).fetchall()
            session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    async def _read_batch_async(
        self,
        queue_name: str,
        vt: int,
        batch_size: int = 1,
    ) -> Optional[List[Message]]:
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                    {
                        "queue_name": queue_name,
                        "vt": vt,
                        "batch_size": batch_size,
                    },
                )
            ).fetchall()
            await session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    def read_batch(
        self,
        queue_name: str,
        batch_size: int = 1,
        vt: Optional[int] = None,
    ) -> Optional[List[Message]]:
        """
        | Read a batch of messages from the queue.
        | Usage:

        Returns:
            List of |schema_message_class|_ or ``None`` if the queue is empty.

        .. code-block:: python

            from pgmq_sqlalchemy.schema import Message

            msgs:List[Message] = pgmq_client.read_batch('my_queue', batch_size=10)
            # with vt
            msgs:List[Message] = pgmq_client.read_batch('my_queue', batch_size=10, vt=10)

        """
        if vt is None:
            vt = self.vt
        if self.is_async:
            return self.loop.run_until_complete(
                self._read_batch_async(queue_name, batch_size, vt)
            )
        return self._read_batch_sync(queue_name, batch_size, vt)

    def _read_with_poll_sync(
        self,
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        with self.session_maker() as session:
            rows = session.execute(
                text(
                    "select * from pgmq.read_with_poll(:queue_name,:vt,:qty,:max_poll_seconds,:poll_interval_ms);"
                ),
                {
                    "queue_name": queue_name,
                    "vt": vt,
                    "qty": qty,
                    "max_poll_seconds": max_poll_seconds,
                    "poll_interval_ms": poll_interval_ms,
                },
            ).fetchall()
            session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    async def _read_with_poll_async(
        self,
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(
                        "select * from pgmq.read_with_poll(:queue_name,:vt,:qty,:max_poll_seconds,:poll_interval_ms);"
                    ),
                    {
                        "queue_name": queue_name,
                        "vt": vt,
                        "qty": qty,
                        "max_poll_seconds": max_poll_seconds,
                        "poll_interval_ms": poll_interval_ms,
                    },
                )
            ).fetchall()
            await session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    def read_with_poll(
        self,
        queue_name: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """

        .. _read_with_poll_method: ref:`pgmq_sqlalchemy.PGMQueue.read_with_poll`
        .. |read_with_poll_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read_with_poll`


        | Read messages from a queue with long-polling.
        |
        | When the queue is empty, the function block at most ``max_poll_seconds`` seconds.
        | During the polling, the function will check the queue every ``poll_interval_ms`` milliseconds, until the queue has ``qty`` messages.

        Args:
            queue_name (str): The name of the queue.
            vt (Optional[int]): The visibility timeout in seconds.
            qty (int): The number of messages to read.
            max_poll_seconds (int): The maximum number of seconds to poll.
            poll_interval_ms (int): The interval in milliseconds to poll.

        Returns:
            List of |schema_message_class|_ or ``None`` if the queue is empty.

        Usage:

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'}, delay=6)

            # the following code will block for 5 seconds
            msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
            assert msgs is None

            # try read_with_poll again
            # the following code will only block for 1 second
            msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
            assert msgs is not None

        Another example:

        .. code-block:: python

            msg = {'key': 'value'}
            msg_ids = pgmq_client.send_batch('my_queue', [msg, msg, msg, msg], delay=3)

            # the following code will block for 3 seconds
            msgs = pgmq_client.read_with_poll('my_queue', qty=3, max_poll_seconds=5, poll_interval_ms=100)
            assert len(msgs) == 3 # will read at most 3 messages (qty=3)

        """
        if vt is None:
            vt = self.vt

        if self.is_async:
            return self.loop.run_until_complete(
                self._read_with_poll_async(
                    queue_name, vt, qty, max_poll_seconds, poll_interval_ms
                )
            )
        return self._read_with_poll_sync(
            queue_name, vt, qty, max_poll_seconds, poll_interval_ms
        )

    def _set_vt_sync(
        self, queue_name: str, msg_id: int, vt_offset: int
    ) -> Optional[Message]:
        """Set the visibility timeout for a message."""
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.set_vt(:queue_name,:msg_id,:vt_offset);"),
                {"queue_name": queue_name, "msg_id": msg_id, "vt_offset": vt_offset},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _set_vt_async(
        self, queue_name: str, msg_id: int, vt_offset: int
    ) -> Optional[Message]:
        """Set the visibility timeout for a message."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.set_vt(:queue_name,:msg_id,:vt_offset);"),
                    {
                        "queue_name": queue_name,
                        "msg_id": msg_id,
                        "vt_offset": vt_offset,
                    },
                )
            ).fetchone()
            await session.commit()
        print("row", row)
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def set_vt(self, queue_name: str, msg_id: int, vt_offset: int) -> Optional[Message]:
        """
        .. _set_vt_method: ref:`pgmq_sqlalchemy.PGMQueue.set_vt`
        .. |set_vt_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.set_vt`

        Set the visibility timeout for a message.

        Args:
            queue_name (str): The name of the queue.
            msg_id (int): The message id.
            vt_offset (int): The visibility timeout in seconds.

        Returns:
            |schema_message_class|_ or ``None`` if the message does not exist.

        Usage:

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'}, delay=10)
            msg = pgmq_client.read('my_queue')
            assert msg is not None
            msg = pgmq_client.set_vt('my_queue', msg.msg_id, 10)
            assert msg is not None

        .. tip::
            | |read_method|_ and |set_vt_method|_ can be used together to implement **exponential backoff** mechanism.
            | `ref: Exponential Backoff And Jitter <https://aws.amazon.com/tw/blogs/architecture/exponential-backoff-and-jitter/>`_.
            | **For example:**

            .. code-block:: python

                from pgmq_sqlalchemy import PGMQueue
                from pgmq_sqlalchemy.schema import Message

                def _exp_backoff_retry(msg: Message)->int:
                    # exponential backoff retry
                    if msg.read_ct < 5:
                        return 2 ** msg.read_ct
                    return 2 ** 5

                def consumer_with_backoff_retry(pgmq_client: PGMQueue, queue_name: str):
                    msg = pgmq_client.read(
                        queue_name=queue_name,
                        vt=1000, # set vt to 1000 seconds temporarily
                    )
                    if msg is None:
                        return

                    # set exponential backoff retry
                    pgmq_client.set_vt(
                        queue_name=query_name,
                        msg_id=msg.msg_id,
                        vt_offset=_exp_backoff_retry(msg)
                    )

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._set_vt_async(queue_name, msg_id, vt_offset)
            )
        return self._set_vt_sync(queue_name, msg_id, vt_offset)

    def _pop_sync(self, queue_name: str) -> Optional[Message]:
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.pop(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _pop_async(self, queue_name: str) -> Optional[Message]:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.pop(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def pop(self, queue_name: str) -> Optional[Message]:
        """
        Reads a single message from a queue and deletes it upon read.

        .. code-block:: python

            msg = pgmq_client.pop('my_queue')
            print(msg.msg_id)
            print(msg.message)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._pop_async(queue_name))
        return self._pop_sync(queue_name)

    def _delete_sync(
        self,
        queue_name: str,
        msg_id: int,
    ) -> bool:
        with self.session_maker() as session:
            # should add explicit type casts to choose the correct candidate function
            row = session.execute(
                text(f"select * from pgmq.delete('{queue_name}',{msg_id}::BIGINT);")
            ).fetchone()
            session.commit()
        return row[0]

    async def _delete_async(
        self,
        queue_name: str,
        msg_id: int,
    ) -> bool:
        async with self.session_maker() as session:
            # should add explicit type casts to choose the correct candidate function
            row = (
                await session.execute(
                    text(f"select * from pgmq.delete('{queue_name}',{msg_id}::BIGINT);")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def delete(self, queue_name: str, msg_id: int) -> bool:
        """
        Delete a message from the queue.

        .. _delete_method: ref:`pgmq_sqlalchemy.PGMQueue.delete`
        .. |delete_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.delete`

        * Raises an error if the ``queue_name`` does not exist.
        * Returns ``True`` if the message is deleted successfully.
        * If the message does not exist, returns ``False``.

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'})
            assert pgmq_client.delete('my_queue', msg_id)
            assert not pgmq_client.delete('my_queue', msg_id)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._delete_async(queue_name, msg_id))
        return self._delete_sync(queue_name, msg_id)

    def _delete_batch_sync(
        self,
        queue_name: str,
        msg_ids: List[int],
    ) -> List[int]:
        # should add explicit type casts to choose the correct candidate function
        with self.session_maker() as session:
            rows = session.execute(
                text(f"select * from pgmq.delete('{queue_name}',ARRAY{msg_ids});")
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _delete_batch_async(
        self,
        queue_name: str,
        msg_ids: List[int],
    ) -> List[int]:
        # should add explicit type casts to choose the correct candidate function
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(f"select * from pgmq.delete('{queue_name}',ARRAY{msg_ids});")
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def delete_batch(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        """
        Delete a batch of messages from the queue.

        .. _delete_batch_method: ref:`pgmq_sqlalchemy.PGMQueue.delete_batch`
        .. |delete_batch_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.delete_batch`

        .. note::
            | Instead of return `bool` like |delete_method|_,
            | |delete_batch_method|_ will return a list of ``msg_id`` that are successfully deleted.

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.delete_batch('my_queue', msg_ids) == msg_ids

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._delete_batch_async(queue_name, msg_ids)
            )
        return self._delete_batch_sync(queue_name, msg_ids)

    def _archive_sync(self, queue_name: str, msg_id: int) -> bool:
        """Archive a message from a queue synchronously."""
        with self.session_maker() as session:
            row = session.execute(
                text(f"select pgmq.archive('{queue_name}',{msg_id}::BIGINT);")
            ).fetchone()
            session.commit()
        return row[0]

    async def _archive_async(self, queue_name: str, msg_id: int) -> bool:
        """Archive a message from a queue asynchronously."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text(f"select pgmq.archive('{queue_name}',{msg_id}::BIGINT);")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def archive(self, queue_name: str, msg_id: int) -> bool:
        """
        Archive a message from a queue.

        .. _archive_method: ref:`pgmq_sqlalchemy.PGMQueue.archive`
        .. |archive_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.archive`


        * Message will be deleted from the queue and moved to the archive table.
            * Will be deleted from ``pgmq.q_<queue_name>`` and be inserted into the ``pgmq.a_<queue_name>`` table.
        * raises an error if the ``queue_name`` does not exist.
        * returns ``True`` if the message is archived successfully.

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'})
            assert pgmq_client.archive('my_queue', msg_id)
            # since the message is archived, queue will be empty
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(self._archive_async(queue_name, msg_id))
        return self._archive_sync(queue_name, msg_id)

    def _archive_batch_sync(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        """Archive multiple messages from a queue synchronously."""
        with self.session_maker() as session:
            rows = session.execute(
                text(f"select * from pgmq.archive('{queue_name}',ARRAY{msg_ids});")
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _archive_batch_async(
        self, queue_name: str, msg_ids: List[int]
    ) -> List[int]:
        """Archive multiple messages from a queue asynchronously."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(f"select * from pgmq.archive('{queue_name}',ARRAY{msg_ids});")
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def archive_batch(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        """
        Archive multiple messages from a queue.

        * Messages will be deleted from the queue and moved to the archive table.
        * Returns a list of ``msg_id`` that are successfully archived.

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.archive_batch('my_queue', msg_ids) == msg_ids
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._archive_batch_async(queue_name, msg_ids)
            )
        return self._archive_batch_sync(queue_name, msg_ids)

    def _purge_sync(self, queue_name: str) -> int:
        """Purge a queue synchronously,return deleted_count."""
        with self.session_maker() as session:
            row = session.execute(
                text("select pgmq.purge_queue(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        return row[0]

    async def _purge_async(self, queue_name: str) -> int:
        """Purge a queue asynchronously,return deleted_count."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select pgmq.purge_queue(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def purge(self, queue_name: str) -> int:
        """
        * Delete all messages from a queue, return the number of messages deleted.
        * Archive tables will **not** be affected.

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.purge('my_queue') == 2
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(self._purge_async(queue_name))
        return self._purge_sync(queue_name)

    def _metrics_sync(self, queue_name: str) -> Optional[QueueMetrics]:
        """Get queue metrics synchronously."""
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.metrics(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )

    async def _metrics_async(self, queue_name: str) -> Optional[QueueMetrics]:
        """Get queue metrics asynchronously."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.metrics(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
        if row is None:
            return None
        return QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )

    def metrics(self, queue_name: str) -> Optional[QueueMetrics]:
        """
        Get metrics for a queue.

        Returns:
            |schema_queue_metrics_class|_ or ``None`` if the queue does not exist.

        Usage:

        .. code-block:: python

            from pgmq_sqlalchemy.schema import QueueMetrics

            metrics:QueueMetrics = pgmq_client.metrics('my_queue')
            print(metrics.queue_name)
            print(metrics.queue_length)
            print(metrics.queue_length)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._metrics_async(queue_name))
        return self._metrics_sync(queue_name)

    def _metrics_all_sync(self) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues synchronously."""
        with self.session_maker() as session:
            rows = session.execute(text("select * from pgmq.metrics_all();")).fetchall()
        if not rows:
            return None
        return [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
            )
            for row in rows
        ]

    async def _metrics_all_async(self) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues asynchronously."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(text("select * from pgmq.metrics_all();"))
            ).fetchall()
        if not rows:
            return None
        return [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
            )
            for row in rows
        ]

    def metrics_all(self) -> Optional[List[QueueMetrics]]:
        """

        .. _read_committed_isolation_level: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
        .. |read_committed_isolation_level| replace:: **READ COMMITTED**

        .. _metrics_all_method: ref:`pgmq_sqlalchemy.PGMQueue.metrics_all`
        .. |metrics_all_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.metrics_all`

        Get metrics for all queues.

        Returns:
            List of |schema_queue_metrics_class|_ or ``None`` if there are no queues.

        Usage:

        .. code-block:: python

            from pgmq_sqlalchemy.schema import QueueMetrics

            metrics:List[QueueMetrics] = pgmq_client.metrics_all()
            for m in metrics:
                print(m.queue_name)
                print(m.queue_length)
                print(m.queue_length)

        .. warning::
            | You should use a **distributed lock** to avoid **race conditions** when calling |metrics_all_method|_ in **concurrent** |drop_queue_method|_ **scenarios**.
            |
            | Since the default PostgreSQL isolation level is |read_committed_isolation_level|_, the queue metrics to be fetched **may not exist** if there are **concurrent** |drop_queue_method|_ **operations**.
            | Check the `pgmq.metrics_all <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L334-L346>`_ function for more details.


        """
        if self.is_async:
            return self.loop.run_until_complete(self._metrics_all_async())
        return self._metrics_all_sync()
