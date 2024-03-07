import json
import logging
import multiprocessing
import time
from multiprocessing import Process
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from tembo_pgmq_python import PGMQueue

logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse

import typer

from benches.ops import consume, produce, queue_depth
from benches.stats import plot_rolling, stack_events, summarize


def bench(
    postgres_connection: str,
    duration_seconds: int,
    bench_name: Optional[str] = None,
    message_size_bytes: int = 1000,
    write_batch_size: int = 1,
    read_batch_size: int = 1,
    read_concurrency: int = 1,
    write_concurrency: int = 1,
    unlogged_queue: bool = False,
    partitioned_queue: bool = False,
    partition_interval: int = 10_000,
    message_retention: int = 1_000_000,
) -> str:
    result = urlparse(postgres_connection)
    connection_info = {
        "username": result.username,
        "password": result.password,
        "host": result.hostname,
        "port": int(result.port),
        "database": result.path.lstrip("/"),
    }

    retention_interval = message_retention

    if bench_name is None:
        time_now = int(time.time())
        bench_name = f"bench_{time_now}"

    bench_params = {
        "bench_name": bench_name,
        "host": connection_info["host"],
        "produce_time_seconds": duration_seconds,
        "read_concurrency": read_concurrency,
        "write_concurrency": write_concurrency,
        "write_batch_size": write_batch_size,
        "read_batch_size": read_batch_size,
        "message_size_bytes": message_size_bytes,
    }

    if partitioned_queue:
        bench_params["partition_interval"] = partition_interval
        bench_params["retention_interval"] = retention_interval

    url = f"postgresql://{connection_info['username']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"  # noqa: E501
    eng = create_engine(url)

    # setup results logging tables
    from benches.log import setup_bench_results

    setup_bench_results(eng, bench_name)

    # capture postgres settings
    query = """
    SELECT name, setting
    FROM pg_settings
    """

    with eng.connect() as con:
        results = con.execute(text(query)).fetchall()
    # Convert results to dictionary
    pg_settings = {row[0]: row[1] for row in results}

    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as con:
        logging.info("Vacuuming")
        con.execute(text("vacuum full"))

    queue = PGMQueue(**connection_info)
    if partitioned_queue:
        logging.info(f"Creating partitioned queue: {bench_name}")
        queue.create_partitioned_queue(
            bench_name, partition_interval=partition_interval, retention_interval=retention_interval
        )
    else:
        logging.info(f"Creating queue: {bench_name}, unlogged: {unlogged_queue}")
        queue.create_queue(bench_name, unlogged=unlogged_queue)

    # run producing and consuming in parallel, separate processes
    producer_procs = {}
    write_kwargs = {
        "queue_name": bench_name,
        "connection_info": connection_info,
        "duration_seconds": duration_seconds,
        "batch_size": write_batch_size,
        "message_size_bytes": message_size_bytes,
    }
    for i in range(write_concurrency):
        producer = f"producer_{i}"
        producer_procs[producer] = Process(target=produce, kwargs=write_kwargs)
        producer_procs[producer].start()

    # start a proc to poll for queue depth
    kill_flag = multiprocessing.Value("b", False)
    queue_depth_proc = Process(target=queue_depth, args=(bench_name, connection_info, kill_flag, duration_seconds))
    queue_depth_proc.start()

    consume_procs = {}
    read_kwargs = {
        "queue_name": bench_name,
        "connection_info": connection_info,
        "pattern": "delete",  # TODO: parameterize this
        "batch_size": read_batch_size,
    }
    for i in range(read_concurrency):
        conumser = f"consumer_{i}"
        consume_procs[conumser] = Process(target=consume, kwargs=read_kwargs)
        consume_procs[conumser].start()

    logging.info("waiting for consumers")
    for consumer, proc in consume_procs.items():
        logging.debug(f"Waiting for {consumer}")
        proc.join()
        logging.debug(f"{consumer} finished")

    logging.info("stopping producers")
    for producer, proc in producer_procs.items():
        logging.debug("Closing: %s", producer)
        proc.terminate()
        logging.debug(f"{producer} finished")

    # stop the queue depth proc
    kill_flag.value = True
    logging.info("Stopping queue depth proc")
    queue_depth_proc.join()

    ## collect and summarize
    event_log = stack_events(bench_name, queue)
    stats_df = summarize(event_log)

    # get dump from pg_stat_statements
    with eng.connect() as c:
        pg_stat_df = pd.read_sql(sql="select * from pg_stat_statements", con=c)

    # write bench summary back to db
    stat_dict = stats_df.set_index("operation").to_dict(orient="index")

    writes = stat_dict["write"]
    reads = stat_dict["read"]
    deletes = stat_dict.get("delete", {})
    archives = stat_dict.get("archive", {})

    bench_summary = {
        "bench_name": bench_name,
        "total_msg_sent": writes["num_messages"],
        "total_msg_read": reads["num_messages"],
        "total_msg_deleted": deletes.get("num_messages"),
        "total_msg_archived": archives.get("num_messages"),
        "server_spec": json.dumps({"todo": "cpu,mem,etc"}),
        "message_size_bytes": message_size_bytes,
        "produce_duration_sec": writes["total_duration_seconds"],
        "consume_duration_sec": reads["total_duration_seconds"],
        "read_concurrency": read_concurrency,
        "write_concurrency": write_concurrency,
        "write_batch_size": write_batch_size,
        "read_batch_size": read_batch_size,
        "pg_settings": json.dumps(pg_settings),
        "latency": json.dumps(
            {
                "write": {"mean": writes["mean"], "stddev": writes["stddev"]},
                "read": {"mean": reads["mean"], "stddev": reads["stddev"]},
                "archive": {"mean": archives.get("mean"), "stddev": archives.get("stddev")},
                "delete": {"mean": deletes.get("mean"), "stddev": deletes.get("stddev")},
            }
        ),
        "throughput": json.dumps(
            {
                "write": {
                    "messages_per_second": writes["messages_per_second"],
                },
                "read": {
                    "messages_per_second": reads["messages_per_second"],
                },
                "archive": {
                    "messages_per_second": archives.get("messages_per_second"),
                },
                "delete": {"messages_per_second": deletes.get("messages_per_second")},
            }
        ),
        "pg_stat_statements": json.dumps(pg_stat_df.to_dict(orient="records")),
    }

    # write to results table
    summary_df = pd.DataFrame([bench_summary])
    with eng.connect() as c:
        summary_df.to_sql("pgmq_bench_results", con=c, if_exists="append", index=None)

    plot_rolling(
        event_log=event_log,
        summary_df=stats_df,
        bench_name=bench_name,
        duration_sec=duration_seconds,
        params=bench_params,
    )

    with eng.connect() as con:
        con.execute(text(f"select pgmq.drop_queue('{bench_name}')"))
        con.commit()

    return bench_name


if __name__ == "__main__":
    # run the concurrency read/write benchmark
    # N processes concurrently sending messages to a single queue
    # M processes concurrently reading messages from the same queue
    # control parameters of the benchmark with the command line arguments provided below

    # example usage (remove `poetry run`) if not using poetry to manage the environment
    # 10 processes each writing 1 message at a time, as fast as possible for 60 seconds
    # 10 processes, each reading->deleting up to 10 messages at a time, until all messages consumed
    # excecute from pgmq/tembo-pgmq-python
    #
    # poetry run python -m benches.bench 'postgresql://$USER:$PASSWORD@$HOST:$PORT/$DATABASE' 60 \
    #   --write-concurrency=10 \
    #   --write-batch-size=1 \
    #   --read-concurrency=10 \
    #   --read-batch-size=10
    typer.run(bench)
