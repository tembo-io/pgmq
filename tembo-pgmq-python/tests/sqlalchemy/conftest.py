import os

import pytest
from pytest import FixtureRequest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session

from tembo_pgmq_python.sqlalchemy.queue import PGMQueue
from .constant import SYNC_DRIVERS, ASYNC_DRIVERS


@pytest.fixture(scope="module")
def get_sa_host():
    return os.getenv("SQLALCHEMY_HOST", "localhost")


@pytest.fixture(scope="module")
def get_sa_port():
    return os.getenv("SQLALCHEMY_PORT", "5432")


@pytest.fixture(scope="module")
def get_sa_user():
    return os.getenv("SQLALCHEMY_USER", "postgres")


@pytest.fixture(scope="module")
def get_sa_password():
    return os.getenv("SQLALCHEMY_PASSWORD", "postgres")


@pytest.fixture(scope="module")
def get_sa_db():
    return os.getenv("SQLALCHEMY_DB", "postgres")


@pytest.fixture(scope="function", params=SYNC_DRIVERS)
def get_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    driver = request.param
    return f"postgresql+{driver}://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}"


@pytest.fixture(scope="function", params=ASYNC_DRIVERS)
def get_async_dsn(
    request: FixtureRequest,
    get_sa_host,
    get_sa_port,
    get_sa_user,
    get_sa_password,
    get_sa_db,
):
    driver = request.param
    return f"postgresql+{driver}://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}"


@pytest.fixture(scope="function")
def get_engine(get_dsn):
    return create_engine(get_dsn)


@pytest.fixture(scope="function")
def get_async_engine(get_async_dsn):
    return create_async_engine(get_async_dsn)


@pytest.fixture(scope="function")
def get_session_maker(get_engine):
    return sessionmaker(bind=get_engine, class_=Session)


@pytest.fixture(scope="function")
def get_async_session_maker(get_async_engine):
    return sessionmaker(bind=get_async_engine, class_=AsyncSession)


@pytest.fixture(scope="function")
def pgmq_by_dsn(get_dsn):
    pgmq = PGMQueue(dsn=get_dsn)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_dsn(get_async_dsn):
    pgmq = PGMQueue(dsn=get_async_dsn)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_engine(get_engine):
    pgmq = PGMQueue(engine=get_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_engine(get_async_engine):
    pgmq = PGMQueue(engine=get_async_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_session_maker(get_session_maker):
    pgmq = PGMQueue(session_maker=get_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_async_session_maker(get_async_session_maker):
    pgmq = PGMQueue(session_maker=get_async_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_dsn_and_engine(get_dsn, get_engine):
    pgmq = PGMQueue(dsn=get_dsn, engine=get_engine)
    return pgmq


@pytest.fixture(scope="function")
def pgmq_by_dsn_and_session_maker(get_dsn, get_session_maker):
    pgmq = PGMQueue(dsn=get_dsn, session_maker=get_session_maker)
    return pgmq


@pytest.fixture(scope="function")
def db_session(get_session_maker) -> Session:
    return get_session_maker()
