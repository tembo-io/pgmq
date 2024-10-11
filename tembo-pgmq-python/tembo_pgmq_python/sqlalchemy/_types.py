from typing import Union, Any, Literal, Tuple, Dict
from collections.abc import Mapping, Sequence
from ._db_api_interface import DBAPICursor

from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

ENGINE_TYPE = Union[Engine, AsyncEngine]
SESSION_TYPE = Union[Session, AsyncSession]
PARAM_STYLE_TYPE = Literal["qmark", "numeric", "named", "format", "pyformat"]
DIALECTS_TYPE = Literal[
    'sqlalchemy',
    'asyncpg',
    'psycopg2',
    'psycopg3',
]
STATEMENT_TYPE = Tuple[TextClause, Dict[str, Any]]

class AsyncDBAPICursor(DBAPICursor):
    async def execute(
        self,
        operation: str,
        parameters: Union[Sequence[Any], Mapping[str, Any]] = ...,
        /,
    ) -> object:
        ...


__all__ = [
    "ENGINE_TYPE",
    "SESSION_TYPE",
    "PARAM_STYLE_TYPE",
    "DIALECTS_TYPE",
    "DBAPICursor",
    "AsyncDBAPICursor",
]
