import pytest
from pgmq_sqlalchemy import PGMQueue

from tests.fixture_deps import pgmq_deps


@pgmq_deps
def test_construct_pgmq(pgmq_fixture):
    pgmq: PGMQueue = pgmq_fixture
    assert pgmq is not None


def test_construct_invalid_pgmq():
    with pytest.raises(ValueError) as e:
        _ = PGMQueue()
    error_msg: str = str(e.value)
    assert "Must provide either dsn, engine, or session_maker" in error_msg
