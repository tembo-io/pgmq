# decorators.py
import functools


def transaction(func):
    """Decorator to run a function within a database transaction."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Determine if 'self' is passed (for methods)
        if args and hasattr(args[0], "pool") and hasattr(args[0], "logger"):
            self = args[0]
            perform_transaction = kwargs.pop("perform_transaction", self.perform_transaction)
            if perform_transaction:
                with self.pool.connection() as conn:
                    txn = conn.transaction()
                    txn.begin()
                    self.logger.debug(f"Transaction started with conn: {conn}")
                    try:
                        result = func(*args, conn=conn, **kwargs)
                        txn.commit()
                        self.logger.debug(f"Transaction committed with conn: {conn}")
                        return result
                    except Exception as e:
                        txn.rollback()
                        self.logger.error(f"Transaction failed with exception: {e}, rolling back.")
                        self.logger.debug(f"Transaction rolled back with conn: {conn}")
                        raise
            else:
                with self.pool.connection() as conn:
                    self.logger.debug(f"Non-transactional execution with conn: {conn}")
                    return func(*args, conn=conn, **kwargs)
        else:
            # For functions without 'self', assume 'queue' is passed explicitly
            queue = kwargs.get("queue") or args[0]
            perform_transaction = kwargs.pop("perform_transaction", queue.perform_transaction)
            if perform_transaction:
                with queue.pool.connection() as conn:
                    txn = conn.transaction()
                    txn.begin()
                    queue.logger.debug(f"Transaction started with conn: {conn}")
                    try:
                        result = func(*args, conn=conn, **kwargs)
                        txn.commit()
                        queue.logger.debug(f"Transaction committed with conn: {conn}")
                        return result
                    except Exception as e:
                        txn.rollback()
                        queue.logger.error(f"Transaction failed with exception: {e}, rolling back.")
                        queue.logger.debug(f"Transaction rolled back with conn: {conn}")
                        raise
            else:
                with queue.pool.connection() as conn:
                    queue.logger.debug(f"Non-transactional execution with conn: {conn}")
                    return func(*args, conn=conn, **kwargs)

    return wrapper


def async_transaction(func):
    """Asynchronous decorator to run a method within a database transaction."""
    import functools

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        perform_transaction = kwargs.pop("perform_transaction", getattr(self, "perform_transaction", False))
        if perform_transaction:
            async with self.pool.acquire() as conn:
                txn = conn.transaction()
                await txn.start()
                try:
                    kwargs["conn"] = conn  # Injecting 'conn' into kwargs
                    result = await func(self, *args, **kwargs)
                    await txn.commit()
                    return result
                except Exception:
                    await txn.rollback()
                    raise
        else:
            async with self.pool.acquire() as conn:
                kwargs["conn"] = conn  # Injecting 'conn' into kwargs
                return await func(self, *args, **kwargs)

    return wrapper
