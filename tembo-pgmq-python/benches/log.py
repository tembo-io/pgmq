from sqlalchemy import text


def setup_bench_results(eng, bench_name: str):
    """Setup logging and temp table"""
    if bench_name is None:
        raise Exception("queue name must not be none")

    # setup results table
    with eng.connect() as con:
        con.execute(
            text(
                f"""
            CREATE TABLE "bench_results_{bench_name}"(
                operation text NULL,
                duration_sec float8 NULL,
                batch_size int8 NULL,
                epoch float8 NULL
            )
        """
            )
        )
        con.commit()

    logging.info("Resetting pg_stat_statements")
    with eng.connect() as con:
        con.execute(text("select pg_stat_statements_reset()")).fetchall()
        con.commit()

    # create table to log all bench results
    with eng.connect() as con:
        con.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS "pgmq_bench_results" (
                bench_name text NOT NULL UNIQUE,
                datetime timestamp with time zone NOT NULL DEFAULT now(),
                total_msg_sent int8 NOT NULL,
                total_msg_read int8 NOT NULL,
                total_msg_deleted int8 NULL,
                total_msg_archived int8 NULL,
                server_spec jsonb NOT NULL,
                message_size_bytes int8 NOT NULL,
                produce_duration_sec int8 NOT NULL,
                consume_duration_sec int8 NOT NULL,
                write_concurrency int8 NOT NULL,
                read_concurrency int8 NOT NULL,
                write_batch_size int8 NOT NULL,
                read_batch_size int8 NOT NULL,
                pg_settings jsonb NOT NULL,
                latency jsonb NOT NULL,
                pg_stat_statements jsonb NULL,
                throughput jsonb NOT NULL
            )
            """
            )
        )
        con.commit()

    # create the bench's event log table
    #
    with eng.connect() as con:
        con.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS "event_log_{bench_name}" (
                operation text NOT NULL,
                duration_sec numeric NULL,
                batch_size numeric NULL,
                epoch numeric NOT NULL,
                queue_length numeric NULL
            )"""
            )
        )
        con.commit()


import logging
import os
import subprocess

import pandas as pd
from sqlalchemy.engine import Engine


def write_event_log(db_url: str, event_log: pd.DataFrame, bench_name: str):
    csv_name = f"/tmp/event_log_{bench_name}.csv"
    event_log.to_csv(csv_name, index=None)
    copy_command = f"\COPY event_log_{bench_name} FROM '{csv_name}' DELIMITER ',' CSV HEADER;"  # noqa
    psql_command = ["psql", db_url, "-c", copy_command]
    subprocess.run(psql_command)
    os.remove(csv_name)
    logging.info(f"Wrote event log to event_log_{bench_name}")
