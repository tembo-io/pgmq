import argparse
import logging
import multiprocessing
import time
from multiprocessing import Process

import pandas as pd
from sqlalchemy import create_engine, text

from tembo_pgmq_python import PGMQueue

logging.basicConfig(level=logging.INFO)

from benches.ops import consume, produce, queue_depth
from benches.stats import summarize, plot_rolling

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
    # poetry run python benches/bench.py \
    #   --postgres_connection='postgresql://$USER:$PASSWORD@$HOST:$PORT/$DATABASE' \
    #   --duration_seconds=60 \
    #   --write_concurrency=10 \
    #   --write_batch_size=1 \
    #   --read_concurrency=10 \
    #   --read_batch_size=10

    parser = argparse.ArgumentParser(description="PGMQ Benchmarking")

    parser.add_argument("--postgres_connection", type=str, required=False, help="postgres connection string")

    parser.add_argument(
        "--duration_seconds", type=int, required=True, help="how long the benchmark should run, in seconds"
    )

    parser.add_argument("--write_batch_size", type=int, default=1, help="number of message per send operation")
    parser.add_argument(
        "--read_batch_size", type=int, default=1, help="number of message per read/delete/archive operation"
    )

    parser.add_argument("--read_concurrency", type=int, default=1, help="number of concurrent consumers")
    parser.add_argument("--write_concurrency", type=int, default=1, help="number of concurrent producers")
    parser.add_argument("--bench_name", type=str, required=False, help="the name of the benchmark")

    parser.add_argument("--unlogged_queue", type=bool, default=False, help="whether to use an unlogged queue")

    # partitioned queue configurations
    parser.add_argument("--partitioned_queue", type=bool, default=False, help="whether to use a partitioned queue")
    parser.add_argument("--partition_interval", type=int, default=10_000, help="number of messages per partition")
    parser.add_argument("--message_retention", type=int, default=1_000_000, help="number of messages per partition")

    args = parser.parse_args()

    # default postgres connection - localhost pgrx
    if args.postgres_connection is None:
        import getpass

        user = getpass.getuser()
        connection_info = dict(host="localhost", port=28815, username=user, password="postgres", database="pgmq")
    else:
        from urllib.parse import urlparse

        result = urlparse(args.postgres_connection)
        connection_info = {
            "username": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": int(result.port),
            "database": result.path.lstrip("/"),
        }

    duration_seconds = args.duration_seconds
    bench_name = args.bench_name

    partitioned_queue = args.partitioned_queue
    partition_interval = args.partition_interval
    retention_interval = args.message_retention

    if bench_name is None:
        bench_name = int(time.time())

    url = f"postgresql://{connection_info['username']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"  # noqa: E501
    eng = create_engine(url)

    # setup results table
    test_queue = f"bench_queue_{bench_name}"
    with eng.connect() as con:
        con.execute(
            text(
                f"""
            CREATE TABLE "bench_results_{test_queue}"(
                operation text NULL,
                duration float8 NULL,
                msg_ids jsonb NULL,
                batch_size int8 NULL,
                epoch float8 NULL
            )
        """
            )
        )
        con.commit()

    with eng.connect() as con:
        con.execute(text("select pg_stat_statements_reset()")).fetchall()
        con.commit()

    # capture postgres settings
    query = """
    SELECT
        name, setting
    FROM
        pg_settings
    WHERE name IN
        (
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
            'autovacuum_analyze_scale_factor',
            'autovacuum_vacuum_cost_limit',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_naptime',
            'random_page_cost',
            'checkpoint_timeout'
        )
    """

    with eng.connect() as con:
        results = con.execute(text(query)).fetchall()

    # Convert results to dictionary
    config_dict = {row[0]: row[1] for row in results}

    queue = PGMQueue(**connection_info)
    if partitioned_queue:
        logging.info(f"Creating partitioned queue: {test_queue}")
        queue.create_partitioned_queue(
            test_queue, partition_interval=partition_interval, retention_interval=retention_interval
        )
    else:
        logging.info(f"Creating queue: {test_queue}, unlogged: {args.unlogged_queue}")
        queue.create_queue(test_queue, unlogged=args.unlogged_queue)

    produce_csv = f"produce_{test_queue}.csv"
    consume_csv = f"consume_{test_queue}.csv"

    # run producing and consuming in parallel, separate processes
    producer_procs = {}
    write_kwargs = {
        "queue_name": test_queue,
        "connection_info": connection_info,
        "duration_seconds": duration_seconds,
        "batch_size": args.write_batch_size,
    }
    for i in range(args.write_concurrency):
        producer = f"producer_{i}"
        producer_procs[producer] = Process(target=produce, kwargs=write_kwargs)
        producer_procs[producer].start()

    # start a proc to poll for queue depth
    kill_flag = multiprocessing.Value("b", False)
    queue_depth_proc = Process(target=queue_depth, args=(test_queue, connection_info, kill_flag, duration_seconds))
    queue_depth_proc.start()

    consume_procs = {}
    read_kwargs = {
        "queue_name": test_queue,
        "connection_info": connection_info,
        "pattern": "delete",  # TODO: parameterize this
        "batch_size": args.read_batch_size,
    }
    for i in range(args.read_concurrency):
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
    queue_depth_proc.join()

    # save pg_stat_statements
    with eng.connect() as con:
        pg_stat_df = pd.read_sql("select * from pg_stat_statements", con=con)
    pg_stat_df.to_sql(f"{bench_name}_pg_stat", index=None, con=eng)

    # once consuming finishes, summarize
    results_file = f"results_{test_queue}.jpg"
    # TODO: organize results in a directory or something, log all the params
    filename = summarize(test_queue, queue, results_file=results_file, duration_seconds=duration_seconds)

    params = {
        "bench_name": bench_name,
        "host": connection_info["host"],
        "produce_time_seconds": duration_seconds,
        "read_concurrency": args.read_concurrency,
        "write_concurrency": args.write_concurrency,
        "write_batch_size": args.write_batch_size,
        "read_batch_size": args.read_batch_size,
    }
    if partitioned_queue:
        params["partition_interval"] = partition_interval
        params["retention_interval"] = retention_interval

    plot_rolling(filename, bench_name, duration_seconds, params=params, pg_settings=config_dict)
