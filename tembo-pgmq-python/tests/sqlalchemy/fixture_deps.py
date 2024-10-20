import uuid
from typing import Tuple

import pytest

from pgmq_sqlalchemy import PGMQueue
from tests._utils import check_queue_exists

LAZY_FIXTURES = [
    pytest.lazy_fixture("pgmq_by_dsn"),
    pytest.lazy_fixture("pgmq_by_async_dsn"),
    pytest.lazy_fixture("pgmq_by_engine"),
    pytest.lazy_fixture("pgmq_by_async_engine"),
    pytest.lazy_fixture("pgmq_by_session_maker"),
    pytest.lazy_fixture("pgmq_by_async_session_maker"),
    pytest.lazy_fixture("pgmq_by_dsn_and_engine"),
    pytest.lazy_fixture("pgmq_by_dsn_and_session_maker"),
]

pgmq_deps = pytest.mark.parametrize(
    "pgmq_fixture",
    LAZY_FIXTURES,
)
"""
Decorator that allows a test function to receive a PGMQueue instance as a parameter.

Usage:
    
```
from tests.fixture_deps import pgmq_deps

@pgmq_deps
def test_create_queue(pgmq_fixture,db_session):
    pgmq:PGMQueue = pgmq_fixture
    # test code here
```

Note:
`pytest` version should < 8.0.0,
or `pytest-lazy-fixture` will not work
ref: https://github.com/TvoroG/pytest-lazy-fixture/issues/65
"""

PGMQ_WITH_QUEUE = Tuple[PGMQueue, str]


@pytest.fixture(scope="function", params=LAZY_FIXTURES)
def pgmq_setup_teardown(request: pytest.FixtureRequest, db_session) -> PGMQ_WITH_QUEUE:
    """
    Fixture that provides a PGMQueue instance with a unique temporary queue with setup and teardown.

    Args:
        request (pytest.FixtureRequest): The pytest fixture request object.
        db_session (sqlalchemy.orm.Session): The SQLAlchemy session object.

    Yields:
        tuple[PGMQueue,str]: A tuple containing the PGMQueue instance and the name of the temporary queue.

    Usage:
        @pgmq_setup_teardown
        def test_something(pgmq_setup_teardown):
            pgmq, queue_name = pgmq_setup_teardown
            # test code here

    """
    pgmq = request.param
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    pgmq.create_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    yield pgmq, queue_name
    pgmq.drop_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is False


@pytest.fixture(scope="function", params=LAZY_FIXTURES)
def pgmq_partitioned_setup_teardown(
    request: pytest.FixtureRequest, db_session
) -> PGMQ_WITH_QUEUE:
    """
    Fixture that provides a PGMQueue instance with a unique temporary partitioned queue with setup and teardown.

    Args:
        request (pytest.FixtureRequest): The pytest fixture request object.
        db_session (sqlalchemy.orm.Session): The SQLAlchemy session object.

    Yields:
        tuple[PGMQueue,str]: A tuple containing the PGMQueue instance and the name of the temporary queue.

    Usage:
        @pgmq_setup_teardown
        def test_something(pgmq_setup_teardown):
            pgmq, queue_name = pgmq_setup_teardown
            # test code here

    """
    pgmq: PGMQueue = request.param
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    assert check_queue_exists(db_session, queue_name) is False
    pgmq.create_partitioned_queue(queue_name)
    assert check_queue_exists(db_session, queue_name) is True
    yield pgmq, queue_name
    pgmq.drop_queue(queue_name, partitioned=True)
    assert check_queue_exists(db_session, queue_name) is False
