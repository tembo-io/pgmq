import multiprocessing
import random
import time
from typing import Optional

import pandas as pd
import psycopg2
from matplotlib import pyplot as plt  # type: ignore
from scipy.ndimage import gaussian_filter1d
from sqlalchemy import create_engine, text

from tembo_pgmq_python import Message, PGMQueue


def bench_send(queue: PGMQueue, queue_name: str, msg: dict, num_messages: int) -> list[dict]:
    all_msg_ids = []
    write_start = time.time()
    results = []
    print("Writing Messages")
    for x in range(num_messages):
        start = time.time()
        msg_id = queue.send(queue_name, msg)
        results.append({"operation": "write", "duration": time.time() - start, "msg_id": msg_id})
        all_msg_ids.append(msg_id)
        if (x + 1) % 10000 == 0:
            print(f"write {x+1} messages")
            elapsed = time.time() - write_start
            avg_write = elapsed / (x + 1)
            print(f"running avg write time (seconds): {avg_write}")
    print(f"Sent {x+1} messages")
    return results


def bench_read_archive(queue: PGMQueue, queue_name: str, num_messages: int) -> list[dict]:
    """Benchmarks the read and archive of messages"""
    read_elapsed = 0.0
    archive_elapsed = 0.0
    results = []
    for x in range(num_messages):
        read_start = time.time()
        message: Message = queue.read(queue_name, vt=2)  # type: ignore
        read_duration = time.time() - read_start
        results.append({"operation": "read", "duration": read_duration, "msg_id": message.msg_id})
        read_elapsed += read_duration

        archive_start = time.time()
        queue.archive(queue_name, message.msg_id)
        archive_duration = time.time() - archive_start
        results.append({"operation": "archive", "duration": archive_duration, "msg_id": message.msg_id})
        archive_elapsed += archive_duration

        if (x + 1) % 10000 == 0:
            avg_read = read_elapsed / (x + 1)
            print(f"read {x+1} messages, avg read time (seconds): {avg_read}")
            avg_archive = archive_elapsed / (x + 1)
            print(f"archived {x+1} messages, avg archive time (seconds): {avg_archive}")
    print(f"Read {x+1} messages")
    return results


def bench_line_item(
    host: str,
    port: str,
    username: str = "postgres",
    num_messages: int = 10000,
    vt=10,
    password: str = "postgres",
    database: str = "postgres",
    partition_interval: int = 10000,
    retention_interval: Optional[int] = None,
) -> list[dict]:
    """records each transaction as a separate line item. Captures results into a list.

    returns:
            [{
                "operation": <operation>,
                "duration": <duration, in seconds>,
                "msg_id": <msg_id>
            }]
    """
    rnd = random.randint(0, 100)
    test_queue = f"bench_queue_{rnd}"
    print(f"Test queue: {test_queue}")

    test_message = {"hello": "world"}
    bench_0_start = time.time()
    queue = PGMQueue(host=host, port=port, username=username, password=password, database=database)
    try:
        print(f"Queue retention: {retention_interval}")
        if retention_interval is None:
            print("Defaulting to retaining all messages: {}")
            retention_interval = num_messages
        queue.create_partitioned_queue(
            test_queue, partition_interval=partition_interval, retention_interval=retention_interval
        )
    except Exception as e:
        print(f"{e}")

    print(
        f"""
    Starting benchmark
    Total messages: {num_messages}
    """
    )

    total_results = []

    # publish messages
    write_results: list[dict] = bench_send(queue, test_queue, test_message, num_messages)
    total_results.extend(write_results)

    # read them all once, each
    print("Reading Messages")
    read_arch_results: list[dict] = bench_read_archive(queue, test_queue, num_messages)
    total_results.extend(read_arch_results)

    # wait for all VT to expire
    while time.time() - bench_0_start < vt:
        print("waiting for all VTs to expire")
        time.sleep(2)

    print("Benchmarking: Message Deletion")
    all_msg_ids = []
    # publish messages
    for x in range(num_messages):
        start = time.time()
        msg_id = queue.send(test_queue, test_message)
        all_msg_ids.append(msg_id)

    print("Deleting Messages")
    for x in all_msg_ids:
        start = time.time()
        queue.delete(test_queue, x)
        total_results.append({"operation": "delete", "duration": time.time() - start, "msg_id": x})

    return total_results


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
    user = connection_info["username"]
    host = connection_info["host"]
    port = connection_info["port"]
    password = connection_info["password"]
    database = connection_info["database"]
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
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
        cur.execute(f"""select * from pgmq_send('{queue_name}', '{{"hello": "world"}}')""")
        msg_id = cur.fetchall()[0][0]
        send_duration = time.perf_counter() - send_start
        all_results.append({"operation": "write", "duration": send_duration, "msg_id": msg_id, "epoch": time.time()})
        num_msg += 1
        running_duration = int(time.time() - start_time)
        # log every 5 seconds
        if time.time() - last_print_time >= 5:
            last_print_time = time.time()
            print(f"Total Messages Sent: {num_msg}, {running_duration} / {duration_seconds} seconds")
    print(f"Total Messages Sent: {num_msg}, {int(running_duration)} / {duration_seconds} seconds")
    df = pd.DataFrame(all_results)
    data_tuples = list(df.itertuples(index=False, name=None))
    insert_query = (
        f"INSERT INTO bench_results_{queue_name} (operation, duration, msg_id, epoch) VALUES (%s, %s, %s, %s);"
    )
    cur.executemany(insert_query, data_tuples)

    cur.close()
    conn.close()
    print("Finished publishing messages")


def consume(queue_name: str, connection_info: dict):
    """Consumes messages from a queue and archives them. Writes results to csv.

    Halts consumption after 5 seconds of no messages.
    """
    url = f"postgresql://{connection_info['username']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    conn = psycopg2.connect(url)
    cur = conn.cursor()

    conn.autocommit = True

    cur = conn.cursor()
    results = []
    no_message_timeout = 0
    while no_message_timeout < 5:
        stmt = f"select * from pgmq_read('{queue_name}', 1, 1)"
        read_start = time.perf_counter()
        cur.execute(stmt)
        # cur.execute("select * from pgmq_read(%s, %s, %s);", [queue_name, 1, 1])
        read_duration = time.perf_counter() - read_start
        message = cur.fetchall()

        if len(message) == 0:
            no_message_timeout += 1
            if no_message_timeout > 2:
                print(f"No messages for {no_message_timeout} consecutive reads")
            time.sleep(0.500)
            continue
        else:
            no_message_timeout = 0
        msg_id = message[0][0]

        results.append({"operation": "read", "duration": read_duration, "msg_id": msg_id, "epoch": time.time()})

        archive_start = time.perf_counter()
        cur.execute("select * from pgmq_archive(%s, %s);", [queue_name, msg_id])
        cur.fetchall()

        archive_duration = time.perf_counter() - archive_start
        results.append({"operation": "archive", "duration": archive_duration, "msg_id": msg_id, "epoch": time.time()})

    num_consumed = len(results) / 2
    print(f"Consumed {num_consumed} messages")

    # divide by 2 because we're appending two results (read/archive) per message
    df = pd.DataFrame(results)
    data_tuples = list(df.itertuples(index=False, name=None))
    print("writing results: ", len(data_tuples))
    insert_query = (
        f"INSERT INTO bench_results_{queue_name} (operation, duration, msg_id, epoch) VALUES (%s, %s, %s, %s);"
    )
    cur.executemany(insert_query, data_tuples)
    cur.close()
    conn.close()


def summarize(queue_name: str, queue: PGMQueue, results_file: str, duration_seconds: int):
    """summarizes results from two csvs into pdf"""

    con = create_engine(f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}")
    df = pd.read_sql(f'''select * from "bench_results_{queue_name}"''', con=con)

    # merge schemas of queue depth so we can plot it in-line with latency results
    queue_depth = pd.read_sql(f'''select * from "bench_results_{queue_name}_queue_depth"''', con=con)
    queue_depth["operation"] = "queue_depth"
    queue_depth.rename(
        columns={
            "total_messages": "msg_id",
            "time": "epoch",
        },
        inplace=True,
    )
    df = pd.concat([df, queue_depth[["operation", "queue_length", "msg_id", "epoch"]]])

    # iteration
    all_results_csv = f"all_results_{queue_name}.csv"
    df.to_csv(all_results_csv, index=False)

    _num_df = df[df["operation"] == "archive"]
    num_messages = _num_df.shape[0]
    # convert seconds to milliseconds
    df["duration"] = df["duration"] * 1000

    for op in ["read", "archive", "write"]:
        _df = df[df["operation"] == op]
        bbplot = _df.boxplot(
            column="duration", by="operation", fontsize=12, layout=(2, 1), rot=90, figsize=(25, 20), return_type="axes"
        )
        title = f"""
        num_messages = {num_messages}
        duration = {duration_seconds}
        """
        bbplot[0].set_title(title)

        filename = f"{op}_{results_file}"
        bbplot[0].get_figure().savefig(filename)
        print("Saved: ", filename)
    return all_results_csv


def plot_rolling(csv: str, bench_name: str, duration_sec: int, params: dict):
    df = pd.read_csv(csv)
    # convert seconds to milliseconds
    df["duration_ms"] = df["duration"] * 1000
    df["time"] = pd.to_datetime(df["epoch"], unit="s")

    max_read = df[df.operation == "read"].time.max()
    result = (
        df[df["time"] <= max_read].groupby("operation").agg({"time": lambda x: x.max() - x.min(), "operation": "size"})
    )
    result.columns = ["range", "num_messages"]
    result.reset_index(inplace=True)

    result.columns = ["operation", "range", "num_messages"]
    result["total_duration_seconds"] = result["range"].apply(lambda x: x.total_seconds())
    result["messages_per_second"] = result["num_messages"] / result["total_duration_seconds"]
    output_str = []
    for _, row in result.iterrows():
        operation = row["operation"]
        if operation == "queue_depth":
            continue
        s = f"{operation}: total seconds: {row['total_duration_seconds']}, total messages: {row['num_messages']}, message / sec = {row['messages_per_second']:.2f}"
        output_str.append(s)

    output_str = "\n".join(output_str)

    # Plotting
    _, ax1 = plt.subplots(figsize=(20, 10))

    # plot the operations
    color_map = {"read": "orange", "write": "blue", "archive": "green"}
    sigma = 1000  # Adjust as needed for the desired smoothing level
    for op in ["read", "write", "archive"]:
        _df = df[df["operation"] == op].sort_values("time")
        ax1.plot(
            _df["time"],
            _df[["duration_ms"]].apply(lambda x: gaussian_filter1d(x, sigma)),
            label=op,
            color=color_map[op],
        )
    ax1.legend(loc="upper left")

    ax1.set_xlabel("time")
    ax1.set_ylabel("Duration (ms)")
    plt.suptitle(f"PGMQ Concurrent Produce/Consumer Benchmark\n{output_str}")
    plt.title(params)
    # Create a second y-axis for 'queue_length'
    ax2 = ax1.twinx()
    queue_depth_data = df[df["time"] <= max_read][df["operation"] == "queue_depth"]
    ax2.plot(queue_depth_data["time"], queue_depth_data["queue_length"], color="gray", label="queue_depth")
    ax2.set_ylabel("queue_depth", color="gray")
    ax2.tick_params("y", colors="gray")

    # Show the plot
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    output_plot = f"{bench_name}_{duration_sec}.png"
    plt.savefig(output_plot)
    print(f"Saved plot to: {output_plot}")


def queue_depth(queue_name: str, connection_info: dict, kill_flag: multiprocessing.Value):
    url = f"postgresql://{connection_info['username']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    conn = psycopg2.connect(url)
    eng = create_engine(url)
    cur = conn.cursor()
    conn.autocommit = True
    all_metrics = []
    while not kill_flag.value:
        cur.execute(f"select * from pgmq_metrics('{queue_name}')")
        metrics = cur.fetchall()[0]
        depth = metrics[1]
        total_messages = metrics[-2]
        all_metrics.append(
            {
                "queue_name": metrics[0],
                "queue_length": depth,
                "total_messages": total_messages,
                "time": time.time(),
            }
        )
        print(f"Number messages in queue: {depth}, max_msg_id: {total_messages}")

        read_start = time.perf_counter()
        cur.execute("select 1")
        cur.fetchall()
        sel_duration = time.perf_counter() - read_start
        print("Select 1 latency (ms): ", sel_duration * 1000)
        time.sleep(5)
    cur.close()
    conn.close()
    print("Writing queue length results")
    df = pd.DataFrame(all_metrics)
    df.to_sql(f"bench_results_{queue_name}_queue_depth", con=eng, if_exists="append", index=False)
    return all_metrics


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

    # default postgres connection
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

    url = f"postgresql://{connection_info['username']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
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
        con.execute(
            text(
                f"""
            select pg_stat_statements_reset()
        """
            )
        ).fetchall()
        con.commit()

    queue = PGMQueue(**connection_info)
    if partitioned_queue:
        print(f"Creating partitioned queue: {test_queue}")
        queue.create_partitioned_queue(
            test_queue, partition_interval=partition_interval, retention_interval=retention_interval
        )
    else:
        print(f"Creating non-partitioned queue: {test_queue}")
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
    queue_depth_proc = Process(target=queue_depth, args=(test_queue, connection_info, kill_flag))
    queue_depth_proc.start()

    consume_procs = {}
    for i in range(args.read_concurrency):
        conumser = f"consumer_{i}"
        consume_procs[conumser] = Process(target=consume, args=(test_queue, connection_info))
        consume_procs[conumser].start()

    for consumer, proc in consume_procs.items():
        print(f"Waiting for {consumer} to finish")
        proc.join()
        print(f"{consumer} finished")

    # stop the queue depth proc
    kill_flag.value = True
    queue_depth_proc.join()

    for producer, proc in producer_procs.items():
        print("Closing producer: ", producer)
        proc.terminate()

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

    plot_rolling(filename, bench_name, duration_seconds, params=params)
