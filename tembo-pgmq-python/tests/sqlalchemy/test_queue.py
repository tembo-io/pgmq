import uuid
import pytest
import time

from sqlalchemy.exc import ProgrammingError
from filelock import FileLock

from tembo_pgmq_python.sqlalchemy.queue import PGMQueue

from tests.sqlalchemy.fixture_deps import (
    pgmq_deps,
    PGMQ_WITH_QUEUE,
    pgmq_setup_teardown,
    pgmq_partitioned_setup_teardown,
)

from tests.sqlalchemy._utils import check_queue_exists
from tests.sqlalchemy.constant import MSG, LOCK_FILE_NAME

use_fixtures = [
    pgmq_setup_teardown,
    pgmq_partitioned_setup_teardown,
]


@pgmq_deps
def test_create_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True


@pgmq_deps
def test_create_partitioned_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.create_partitioned_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True


def test_create_same_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE, db_session):
    pgmq, queue_name = pgmq_setup_teardown
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    pgmq.create_queue(queue_name)
    # `create_queue` with the same queue name should not raise an exception
    # and the queue should still exist
    assert check_queue_exists(db_session, queue_name) is True


@pgmq_deps
def test_validate_queue_name(pgmq_fixture):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    pgmq.validate_queue_name(queue_name)
    # `queue_name` should be a less than 48 characters
    with pytest.raises(Exception) as e:
        pgmq.validate_queue_name("a" * 49)
    error_msg: str = str(e.value.orig)
    assert "queue name is too long, maximum length is 48 characters" in error_msg


def test_drop_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    _ = pgmq_setup_teardown
    pass


@pgmq_deps
def test_drop_non_exist_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    with pytest.raises(ProgrammingError):
        pgmq.drop_queue(queue_name)


def test_drop_partitioned_queue(pgmq_partitioned_setup_teardown: PGMQ_WITH_QUEUE):
    _ = pgmq_partitioned_setup_teardown
    pass


@pgmq_deps
def test_drop_non_exist_partitioned_queue(pgmq_fixture, db_session):
    pgmq: PGMQueue = pgmq_fixture
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    with pytest.raises(ProgrammingError):
        pgmq.drop_queue(queue_name, partitioned=True)


def test_list_queues(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    queues = pgmq.list_queues()
    assert queue_name in queues


def test_list_partitioned_queues(pgmq_partitioned_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_partitioned_setup_teardown
    queues = pgmq.list_queues()
    assert queue_name in queues


def test_send_and_read_msg(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_id: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_delay(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_id: int = pgmq.send(queue_name, msg, delay=2)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(1)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(1.1)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_vt(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_id: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id
    time.sleep(1.5)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(0.6)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_send_and_read_msg_with_vt_and_delay(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_id: int = pgmq.send(queue_name, msg, delay=2)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read is None
    time.sleep(1)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read is None
    time.sleep(1.1)
    msg_read = pgmq.read(queue_name, vt=2)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id
    time.sleep(1.5)
    msg_read = pgmq.read(queue_name)
    assert msg_read is None
    time.sleep(0.6)
    msg_read = pgmq.read(queue_name)
    assert msg_read.message == msg
    assert msg_read.msg_id == msg_id


def test_read_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg_read = pgmq.read(queue_name)
    assert msg_read is None


def test_read_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_id_1: int = pgmq.send(queue_name, msg)
    msg_id_2: int = pgmq.send(queue_name, msg)
    msg_read = pgmq.read_batch(queue_name, 3)
    assert len(msg_read) == 2
    assert msg_read[0].message == msg
    assert msg_read[0].msg_id == msg_id_1
    assert msg_read[1].message == msg
    assert msg_read[1].msg_id == msg_id_2


def test_read_batch_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg_read = pgmq.read_batch(queue_name, 3)
    assert msg_read is None


def test_send_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name=queue_name, messages=[msg, msg, msg])
    assert len(msg_ids) == 3
    assert msg_ids == [1, 2, 3]


def test_send_batch_with_read_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name=queue_name, messages=[msg, msg, msg])
    assert len(msg_ids) == 3
    assert msg_ids == [1, 2, 3]
    msg_read_batch = pgmq.read_batch(queue_name, 3)
    assert len(msg_read_batch) == 3
    assert [msg_read.message for msg_read in msg_read_batch] == [msg, msg, msg]
    assert [msg_read.msg_id for msg_read in msg_read_batch] == [1, 2, 3]


def test_read_with_poll(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg, msg, msg], delay=2)
    start_time = time.time()
    msg_reads = pgmq.read_with_poll(
        queue_name,
        vt=1000,
        qty=3,
        max_poll_seconds=5,
        poll_interval_ms=1001,
    )
    end_time = time.time()
    duration = end_time - start_time
    assert len(msg_reads) == 3
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids[:3]
    assert duration < 5 and duration > 2


def test_read_with_poll_with_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    start_time = time.time()
    msg_reads = pgmq.read_with_poll(
        queue_name,
        vt=1000,
        qty=3,
        max_poll_seconds=2,
        poll_interval_ms=100,
    )
    end_time = time.time()
    duration = end_time - start_time
    assert msg_reads is None
    assert duration > 1.9


def test_pop(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    msg = pgmq.pop(queue_name)
    assert msg.msg_id == msg_ids[0]
    assert msg.message == MSG
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 2
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids[1:]


def test_pop_empty_queue(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = pgmq.pop(queue_name)
    assert msg is None


def test_delete_msg(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.delete(queue_name, msg_ids[1]) is True
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 2
    assert [msg_read.msg_id for msg_read in msg_reads] == [msg_ids[0], msg_ids[2]]


def test_delete_msg_not_exist(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.delete(queue_name, 999) is False
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 3
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids


def test_delete_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.delete_batch(queue_name, [msg_ids[0], msg_ids[2]]) == [
        msg_ids[0],
        msg_ids[2],
    ]
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 1
    assert [msg_read.msg_id for msg_read in msg_reads] == [msg_ids[1]]


def test_delete_batch_not_exist(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.delete_batch(queue_name, [999, 998]) == []
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 3
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids


def test_archive(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.archive(queue_name, msg_ids[0]) is True
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 2
    assert [msg_read.msg_id for msg_read in msg_reads] == [msg_ids[1], msg_ids[2]]


def test_archive_not_exist(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.archive(queue_name, 999) is False
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 3
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids


def test_archive_batch(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.archive_batch(queue_name, [msg_ids[0], msg_ids[2]]) == [
        msg_ids[0],
        msg_ids[2],
    ]
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 1
    assert [msg_read.msg_id for msg_read in msg_reads] == [msg_ids[1]]


def test_archive_batch_not_exist(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    msg_ids = pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.archive_batch(queue_name, [999, 998]) == []
    msg_reads = pgmq.read_batch(queue_name, 3)
    assert len(msg_reads) == 3
    assert [msg_read.msg_id for msg_read in msg_reads] == msg_ids


def test_purge(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    msg = MSG
    assert pgmq.purge(queue_name) == 0
    pgmq.send_batch(queue_name, [msg, msg, msg])
    assert pgmq.purge(queue_name) == 3


def test_metrics(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    pgmq, queue_name = pgmq_setup_teardown
    metrics = pgmq.metrics(queue_name)
    assert metrics is not None
    assert metrics.queue_name == queue_name
    assert metrics.queue_length == 0
    assert metrics.newest_msg_age_sec is None
    assert metrics.oldest_msg_age_sec is None
    assert metrics.total_messages == 0


def test_metrics_all_queues(pgmq_setup_teardown: PGMQ_WITH_QUEUE):
    # Since default PostgreSQL isolation level is `READ COMMITTED`,
    # pytest-xdist is running in **muti-process** mode, which causes **Phantom read** !
    # - `pgmq.metrics_all()` will first get the queue list, then get the metrics for each queue
    # - If another process teardown the queue before the metrics are fetched, will throw an exception that the `{queue_name}` does not exist
    with FileLock(LOCK_FILE_NAME):
        pgmq, queue_name_1 = pgmq_setup_teardown
        queue_name_2 = f"test_queue_{uuid.uuid4().hex}"
        pgmq.create_queue(queue_name_2)
        pgmq.send_batch(queue_name_1, [MSG, MSG, MSG])
        pgmq.send_batch(queue_name_2, [MSG, MSG])
        metrics_all = pgmq.metrics_all()
        queue_1 = [q for q in metrics_all if q.queue_name == queue_name_1][0]
        queue_2 = [q for q in metrics_all if q.queue_name == queue_name_2][0]
        assert queue_1.queue_length == 3
        assert queue_2.queue_length == 2
        assert queue_1.total_messages == 3
        assert queue_2.total_messages == 2
