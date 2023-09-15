import json
import logging
import multiprocessing
import os
import subprocess
import time

import pandas as pd
import psycopg2
from matplotlib import pyplot as plt  # type: ignore
from scipy.ndimage import gaussian_filter1d
from sqlalchemy import create_engine, text

from tembo_pgmq_python import PGMQueue

logging.basicConfig(level=logging.INFO)


def produce(
    queue_name: str,
    connection_info: dict,
    duration_seconds: int = 60,
):
    """Publishes messages at a given rate for a given duration
    Assumes queue_name already exists. Writes results to csv.

    Args:
        queue_name: The name of the queue to publish to
        duration_seconds: The number of seconds to publish messages
    """
    pid = os.getpid()
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    all_results = []

    start_time = time.time()

    num_msg = 0
    running_duration = 0
    last_print_time = time.time()

    while running_duration < duration_seconds:
        send_start = time.perf_counter()
        cur.execute(f"""select * from pgmq.send('{queue_name}', '{{"hello": "world"}}')""")
        msg_id = cur.fetchall()[0][0]
        send_duration = time.perf_counter() - send_start
        all_results.append({"operation": "write", "duration": send_duration, "msg_id": msg_id, "epoch": time.time()})
        num_msg += 1
        running_duration = int(time.time() - start_time)
        # log every 5 seconds
        if time.time() - last_print_time >= 5:
            last_print_time = time.time()
            logging.debug(f"pid: {pid}, total_sent: {num_msg}, {running_duration} / {duration_seconds} seconds")
    cur.close()
    conn.close()
    logging.debug(f"pid: {pid}, total_sent: {num_msg}, {running_duration} / {duration_seconds} seconds")

    csv_name = f"/tmp/tmp_produce_{pid}_{queue_name}.csv"
    df = pd.DataFrame(all_results)
    df.to_csv(csv_name, index=None)
    copy_command = f"\COPY bench_results_{queue_name} FROM '{csv_name}' DELIMITER ',' CSV HEADER;"  # noqa
    psql_command = ["psql", url, "-c", copy_command]
    subprocess.run(psql_command)
    os.remove(csv_name)
    logging.info(f"producer complete, pid: {pid}")


def consume(queue_name: str, connection_info: dict):
    """Consumes messages from a queue and archives them. Writes results to csv.

    Halts consumption after 5 seconds of no messages.
    """
    pid = os.getpid()
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    conn = psycopg2.connect(url)
    cur = conn.cursor()

    conn.autocommit = True

    cur = conn.cursor()
    results = []
    no_message_timeout = 0
    while no_message_timeout < 5:
        stmt = f"select * from pgmq.read('{queue_name}', 1, 1)"
        read_start = time.perf_counter()
        cur.execute(stmt)
        # cur.execute("select * from pgmq.read(%s, %s, %s);", [queue_name, 1, 1])
        read_duration = time.perf_counter() - read_start
        message = cur.fetchall()

        if len(message) == 0:
            no_message_timeout += 1
            if no_message_timeout > 2:
                logging.debug(f"No messages for {no_message_timeout} consecutive reads")
            time.sleep(0.500)
            continue
        else:
            no_message_timeout = 0
        msg_id = message[0][0]

        results.append({"operation": "read", "duration": read_duration, "msg_id": msg_id, "epoch": time.time()})

        archive_start = time.perf_counter()
        cur.execute("select * from pgmq.archive(%s, %s);", [queue_name, msg_id])
        cur.fetchall()

        archive_duration = time.perf_counter() - archive_start
        results.append({"operation": "archive", "duration": archive_duration, "msg_id": msg_id, "epoch": time.time()})
    cur.close()
    conn.close()

    # divide by 2 because we're appending two results (read/archive) per message
    num_consumed = len(results) / 2
    logging.info(f"pid: {pid}, read {num_consumed} messages")

    df = pd.DataFrame(results)
    csv_name = f"/tmp/tmp_consume_{pid}_{queue_name}.csv"
    df.to_csv(csv_name, index=None)
    copy_command = f"\COPY bench_results_{queue_name} FROM '{csv_name}' DELIMITER ',' CSV HEADER;"  # noqa: W605
    psql_command = ["psql", url, "-c", copy_command]
    subprocess.run(psql_command)
    os.remove(csv_name)


def queue_depth(queue_name: str, connection_info: dict, kill_flag: multiprocessing.Value, duration_seconds: int):
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    conn = psycopg2.connect(url)
    pid = os.getpid()
    cur = conn.cursor()
    conn.autocommit = True
    all_metrics = []

    cur.execute(
        f"""
        CREATE TABLE bench_results_{queue_name}_queue_depth(
            queue_name text NULL,
            queue_length int8 NULL,
            total_messages int8 NULL,
            select1 float8 NULL,
            "time" float8 NULL
        )"""
    )

    start = time.time()
    while not kill_flag.value:
        cur.execute(f"select * from pgmq.metrics('{queue_name}')")
        metrics = cur.fetchall()[0]
        depth = metrics[1]
        total_messages = metrics[-2]

        read_start = time.perf_counter()
        cur.execute("select 1")
        cur.fetchall()
        sel_duration = time.perf_counter() - read_start
        duration = int(time.time() - start)
        all_metrics.append(
            {
                "queue_name": metrics[0],
                "queue_length": depth,
                "total_messages": total_messages,
                "select1": sel_duration,
                "time": time.time(),
            }
        )
        log = {
            "q_len": depth,
            "elapsed": f"{duration}/{duration_seconds}",
            "select1_ms": round(sel_duration * 1000, 2),
            "tot_msg": total_messages,
        }
        logging.info(log)
        time.sleep(5)
    cur.close()
    conn.close()
    df = pd.DataFrame(all_metrics)
    csv_name = f"/tmp/tmp_consume_{pid}_{queue_name}.csv"
    df.to_csv(csv_name, index=None)
    copy_command = (
        f"\COPY bench_results_{queue_name}_queue_depth FROM '{csv_name}' DELIMITER ',' CSV HEADER;"  # noqa: W605
    )
    psql_command = ["psql", url, "-c", copy_command]
    subprocess.run(psql_command)
    os.remove(csv_name)

    return all_metrics


def summarize(
    queue_name: str, queue: PGMQueue, results_file: str, duration_seconds: int, boxplots: bool = False
) -> str:
    """summarizes bench results from postgres"""

    con = create_engine(f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}")
    df = pd.read_sql(f'''select * from "bench_results_{queue_name}"''', con=con)

    # merge schemas of queue depth so we can plot it in-line with latency results
    queue_depth = pd.read_sql(f'''select * from "bench_results_{queue_name}_queue_depth"''', con=con)

    sel1_df = queue_depth[["time", "select1"]]
    sel1_df["operation"] = "select1"
    sel1_df.rename(
        columns={
            "select1": "duration",
            "time": "epoch",
        },
        inplace=True,
    )
    queue_depth["operation"] = "queue_depth"
    queue_depth.rename(
        columns={
            "total_messages": "msg_id",
            "time": "epoch",
        },
        inplace=True,
    )
    df = pd.concat([df, sel1_df, queue_depth[["operation", "queue_length", "msg_id", "epoch"]]])

    # iteration
    all_results_csv = f"all_results_{queue_name}.csv"
    df.to_csv(all_results_csv, index=False)

    _num_df = df[df["operation"] == "archive"]
    num_messages = _num_df.shape[0]
    # convert seconds to milliseconds
    df["duration"] = df["duration"] * 1000

    if boxplots:
        for op in ["read", "archive", "write"]:
            _df = df[df["operation"] == op]
            bbplot = _df.boxplot(
                column="duration",
                by="operation",
                fontsize=12,
                layout=(2, 1),
                rot=90,
                figsize=(25, 20),
                return_type="axes",
            )
            title = f"""
            num_messages = {num_messages}
            duration = {duration_seconds}
            """
            bbplot[0].set_title(title)

            filename = f"{op}_{results_file}"
            bbplot[0].get_figure().savefig(filename)
            logging.info("Saved: %s", filename)
    return all_results_csv


def plot_rolling(csv: str, bench_name: str, duration_sec: int, params: dict, pg_settings: dict):
    df = pd.read_csv(csv)
    # convert seconds to milliseconds
    df["duration_ms"] = df["duration"] * 1000
    df["time"] = pd.to_datetime(df["epoch"], unit="s")
    result = df.groupby("operation").agg({"time": lambda x: x.max() - x.min(), "operation": "size"})
    result.columns = ["range", "num_messages"]
    result.reset_index(inplace=True)

    result.columns = ["operation", "range", "num_messages"]
    result["total_duration_seconds"] = result["range"].apply(lambda x: x.total_seconds())
    result["messages_per_second"] = result["num_messages"] / result["total_duration_seconds"]

    def int_to_comma_string(n):
        return "{:,}".format(n)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(20, 10))
    plt.suptitle("PGMQ Concurrent Produce/Consumer Benchmark")
    ax1.text(0, -0.05, json.dumps(pg_settings, indent=2), transform=ax1.transAxes, va="top", ha="left")
    ax1.text(0.85, -0.05, json.dumps(params, indent=2), transform=ax1.transAxes, va="top", ha="left")

    # Prepare the throughput table
    columns = ["Operation", "Duration (s)", "Total Messages", "msg/s"]
    cell_text = []
    for _, row in result.iterrows():
        operation = row["operation"]
        if operation in ["queue_depth", "select1"]:
            continue
        dur = int_to_comma_string(int(row["total_duration_seconds"]))
        n_msg = int_to_comma_string(row["num_messages"])
        msg_per_sec = int_to_comma_string(int(row["messages_per_second"]))
        cell_text.append([operation, dur, n_msg, msg_per_sec])
    table = ax1.table(cellText=cell_text, colLabels=columns, loc="top", cellLoc="left")
    table.set_fontsize(16)
    for i, _ in enumerate(columns):
        table.auto_set_column_width(i)
        for row in range(4):
            table[(row, i)].set_height(0.05)
    fig.subplots_adjust(top=0.8)

    # plot the operations
    color_map = {"read": "orange", "write": "blue", "archive": "green", "select1": "red"}
    sigma = 1000  # Adjust as needed for the desired smoothing level
    for op in ["read", "write", "archive", "select1"]:
        _df = df[df["operation"] == op].sort_values("time")

        if op != "select1":
            y_data = _df[["duration_ms"]].apply(lambda x: gaussian_filter1d(x, sigma))
        else:
            y_data = _df[["duration_ms"]]
        ax1.plot(
            _df["time"],
            y_data,
            label=op,
            color=color_map[op],
        )
    ax1.legend(loc="upper left")

    ax1.set_xlabel("time")
    ax1.set_ylabel("Duration (ms)")

    # Create a second y-axis for 'queue_length'
    ax2 = ax1.twinx()
    queue_depth_data = df[df["operation"] == "queue_depth"]
    ax2.plot(queue_depth_data["time"], queue_depth_data["queue_length"], color="gray", label="queue_depth")
    ax2.set_ylabel("queue_depth", color="gray")
    ax2.tick_params("y", colors="gray")

    # Show the plot
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    fig.subplots_adjust(bottom=0.25)

    output_plot = f"{bench_name}_{duration_sec}.png"
    plt.savefig(output_plot)
    logging.info(f"Saved plot to: {output_plot}")


if __name__ == "__main__":
    # run the multiproc benchmark
    # 1 process publishing messages
    # another process reading and archiving messages
    # both write results to csv
    # script merges csvs and summarizes results
    import argparse
    from multiprocessing import Process

    parser = argparse.ArgumentParser(description="PGMQ Benchmarking")

    parser.add_argument("--postgres_connection", type=str, required=False, help="postgres connection string")

    parser.add_argument(
        "--duration_seconds", type=int, required=True, help="how long the benchmark should run, in seconds"
    )
    parser.add_argument("--read_concurrency", type=int, default=1, help="number of concurrent consumers")
    parser.add_argument("--write_concurrency", type=int, default=1, help="number of concurrent producers")
    parser.add_argument("--bench_name", type=str, required=False, help="the name of the benchmark")

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
                msg_id int8 NULL,
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
        logging.debug(f"Creating partitioned queue: {test_queue}")
        queue.create_partitioned_queue(
            test_queue, partition_interval=partition_interval, retention_interval=retention_interval
        )
    else:
        logging.debug(f"Creating non-partitioned queue: {test_queue}")
        queue.create_queue(
            test_queue,
        )

    produce_csv = f"produce_{test_queue}.csv"
    consume_csv = f"consume_{test_queue}.csv"

    # run producing and consuming in parallel, separate processes
    producer_procs = {}
    for i in range(args.write_concurrency):
        producer = f"producer_{i}"
        producer_procs[producer] = Process(target=produce, args=(test_queue, connection_info, duration_seconds))
        producer_procs[producer].start()

    # start a proc to poll for queue depth
    kill_flag = multiprocessing.Value("b", False)
    queue_depth_proc = Process(target=queue_depth, args=(test_queue, connection_info, kill_flag, duration_seconds))
    queue_depth_proc.start()

    consume_procs = {}
    for i in range(args.read_concurrency):
        conumser = f"consumer_{i}"
        consume_procs[conumser] = Process(target=consume, args=(test_queue, connection_info))
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
    }
    if partitioned_queue:
        params["partition_interval"] = partition_interval
        params["retention_interval"] = retention_interval

    plot_rolling(filename, bench_name, duration_seconds, params=params, pg_settings=config_dict)
