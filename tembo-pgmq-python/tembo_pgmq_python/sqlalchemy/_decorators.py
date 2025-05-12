from typing import TYPE_CHECKING
import functools

if TYPE_CHECKING:
    from tembo_pgmq_python.sqlalchemy.queue import PGMQueue


def inject_session(func):
    """Decorator to inject sqlalchemy session into a method if session not provided."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: "PGMQueue" = args[0]
        if "session" not in kwargs or kwargs["session"] is None:
            if self.transaction_mode:
                raise Exception("Session must be provided in transaction mode")
            session = self.session_maker()
            kwargs["session"] = session
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
        else:
            return func(*args, **kwargs)

    return wrapper


def inject_async_session(func):
    """Asynchronous decorator to inject sqlalchemy session into a method if session not provided."""

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        self: "PGMQueue" = args[0]
        if "session" not in kwargs or kwargs["session"] is None:
            if self.transaction_mode:
                raise Exception("AsyncSession must be provided in transaction mode")
            session = await self.session_maker()
            kwargs["session"] = session
            try:
                result = await func(self, *args, **kwargs)
                return result
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                session.close()

    return wrapper
