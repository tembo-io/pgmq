from typing import TYPE_CHECKING
import functools

if TYPE_CHECKING:
    from tembo_pgmq_python.sqlalchemy.queue import PGMQueue

def inject_session(func):
    """Decorator to inject sqlalchemy session into a method if session not provided."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self:"PGMQueue" = args[0]
        if "session" not in kwargs:
            session = self.session_maker()
            kwargs["session"] = session
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()


    return wrapper

def inject_async_session(func):
    """Asynchronous decorator to inject sqlalchemy session into a method if session not provided."""

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        self:"PGMQueue" = args[0]
        if "session" not in kwargs:
            session = await self.session_maker()
            kwargs["session"] = session
            try:
                result = await func(self, *args, **kwargs)
                return result
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()

    return wrapper