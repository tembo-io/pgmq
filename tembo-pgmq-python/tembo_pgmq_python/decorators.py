# decorators.py
import functools


def transaction(func):
    """Decorator to run a function within a database transaction."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if args and hasattr(args[0], "pool") and hasattr(args[0], "logger"):
            self = args[0]

            if "conn" not in kwargs:
                with self.pool.connection() as conn:
                    with conn.transaction():
                        self.logger.debug(f"Transaction started with conn: {conn}")
                        try:
                            kwargs["conn"] = conn  # Inject 'conn' into kwargs
                            result = func(*args, **kwargs)
                            self.logger.debug(f"Transaction completed with conn: {conn}")
                            return result
                        except Exception as e:
                            self.logger.error(f"Transaction failed with exception: {e}, rolling back.")
                            raise
            else:
                return func(*args, **kwargs)

        else:
            queue = kwargs.get("queue") or args[0]

            if "conn" not in kwargs:
                with queue.pool.connection() as conn:
                    with conn.transaction():
                        queue.logger.debug(f"Transaction started with conn: {conn}")
                        try:
                            kwargs["conn"] = conn  # Inject 'conn' into kwargs
                            result = func(*args, **kwargs)
                            queue.logger.debug(f"Transaction completed with conn: {conn}")
                            return result
                        except Exception as e:
                            queue.logger.error(f"Transaction failed with exception: {e}, rolling back.")
                            raise
            else:
                return func(*args, **kwargs)

    return wrapper


def async_transaction(func):
    """Asynchronous decorator to run a method within a database transaction."""

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if "conn" not in kwargs:
            async with self.pool.acquire() as conn:
                txn = conn.transaction()
                await txn.start()
                try:
                    kwargs["conn"] = conn
                    result = await func(self, *args, **kwargs)
                    await txn.commit()
                    return result
                except Exception:
                    await txn.rollback()
                    raise
        else:
            return await func(self, *args, **kwargs)

    return wrapper
