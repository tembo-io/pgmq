SYNC_DRIVERS = [
    # "pg8000",
    "psycopg2",
    "psycopg",
    # "psycopg2cffi",
]

ASYNC_DRIVERS = [
    "asyncpg",
]

DRIVERS = [
    # "pg8000",
    "psycopg2",
    "psycopg",
    "asyncpg",
    # "psycopg2cffi",
]

MSG = {
    "foo": "bar",
    "hello": "world",
}

LOCK_FILE_NAME = "pgmq.meta.lock.txt"
