import json
from typing import List

from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession

from tembo_pgmq_python.sqlalchemy._types import ENGINE_TYPE, SESSION_TYPE


def get_session_type(engine: ENGINE_TYPE) -> SESSION_TYPE:
    if engine.dialect.is_async:
        return AsyncSession
    return Session


def is_async_session_maker(session_maker: sessionmaker) -> bool:
    return AsyncSession in session_maker.class_.__mro__


def is_async_dsn(dsn: str) -> bool:
    return dsn.startswith("postgresql+asyncpg")


def encode_dict_to_psql(msg: dict) -> str:
    return f"'{json.dumps(msg)}'::jsonb"


def encode_list_to_psql(messages: List[dict]) -> str:
    return f"ARRAY[{','.join([encode_dict_to_psql(msg) for msg in messages])}]"
