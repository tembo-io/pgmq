import random
import time
from typing import Optional

import pandas as pd
from matplotlib import pyplot as plt  # type: ignore
from sqlalchemy import create_engine

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
    tps: int = 500,
):
    """Publishes messages at a given rate for a given duration
    Assumes queue_name already exists. Writes results to csv.

    Args:
        queue_name: The name of the queue to publish to
        duration_seconds: The number of seconds to publish messages
        tps: The number of messages to publish per second
    """
    queue = PGMQueue(**connection_info)

    msg = {"hello": "world"}
    # delay between messages, assuming instantaneous send
    delay = 1.0 / tps

    total_messages = tps * duration_seconds

    start_time = time.time()

    all_results = []
    for i in range(int(total_messages)):
        send_start = time.time()
        msg_id: int = queue.send(queue_name, msg)
        send_duration = time.time() - send_start
        all_results.append(
            {"operation": "write", "duration": round(send_duration, 4), "msg_id": msg_id, "epoch": send_start}
        )
        # Sleep to maintain the desired tps
        time.sleep(delay - ((time.time() - start_time) % delay))
        if i % 10000 == 0:
            print(f"Sent {i} / {int(total_messages)} messages")

    df = pd.DataFrame(all_results)
    con = create_engine(f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}")
    df.to_sql(f"bench_results_{queue_name}", con=con, if_exists="append", index=False)
    # df.to_csv(f"produce_{queue_name}.csv", index=False)


def consume(queue_name: str, connection_info: dict):
    """Consumes messages from a queue and archives them. Writes results to csv.

    Halts consumption after 5 seconds of no messages.
    """
    queue = PGMQueue(**connection_info)

    results = []
    no_message_timeout = 0
    while no_message_timeout < 5:
        read_start = time.time()
        message: Optional[Message] = queue.read(queue_name, vt=10)
        if message is None:
            no_message_timeout += 1
            if no_message_timeout > 2:
                print(f"No messages for {no_message_timeout} consecutive reads")
            time.sleep(0.500)
            continue
        else:
            no_message_timeout = 0

        read_duration = time.time() - read_start
        results.append({"operation": "read", "duration": read_duration, "msg_id": message.msg_id, "epoch": read_start})

        archive_start = time.time()
        queue.archive(queue_name, message.msg_id)
        archive_duration = time.time() - archive_start
        results.append(
            {"operation": "archive", "duration": archive_duration, "msg_id": message.msg_id, "epoch": archive_start}
        )

    # divide by 2 because we're appending two results (read/archive) per message
    num_consumed = len(results) / 2
    print(f"Consumed {num_consumed} messages")
    df = pd.DataFrame(results)
    con = create_engine(f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}")
    df.to_sql(f"bench_results_{queue_name}", con=con, if_exists="append", index=False)
    # df.to_csv(f"consume_{queue_name}.csv", index=False)


def summarize(queue_name: str, queue: PGMQueue, results_file: str, duration_seconds: int, tps: int):
    """summarizes results from two csvs into pdf"""

    con = create_engine(f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}")
    df = pd.read_sql(f"select * from bench_results_{queue_name}", con=con)

    # iteration
    trial = queue_name

    all_results_csv = f"all_results_{trial}.csv"
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

        bbplot[0].set_ylabel("Milliseconds")

        title = f"""
        num_messages = {num_messages}
        duration = {duration_seconds}
        tps = {tps}
        """
        bbplot[0].set_title(title)

        filename = f"{op}_{results_file}"
        bbplot[0].get_figure().savefig(filename)
        print("Saved: ", filename)
    return all_results_csv


def generate_plot(csv_name: str, bench_name: str, duration: int, tps: int, params: dict, window: int = 10_000) -> None:
    alldf = pd.read_csv(csv_name)
    alldf["duration_ms"] = alldf["duration"] * 1000
    wide_df = pd.pivot(alldf, index="msg_id", columns="operation", values="duration_ms")
    ax = wide_df.rolling(window).mean().plot(figsize=(20, 10))
    ax.set_xlabel("Message Number")
    ax.set_ylabel("Duration (ms)")
    plt.suptitle("PGMQ Concurrent Produce/Consumer Benchmark")
    plt.title(params)
    output_plot = f"rolling_avg_{duration}_{tps}_{bench_name}.png"
    plt.savefig(output_plot)
    print(f"Saved plot to: {output_plot}")


if __name__ == "__main__":
    # run the multiproc benchmark
    # 1 process publishing messages
    # another process reading and archiving messages
    # both write results to csv
    # script merges csvs and summarizes results
    import argparse
    from multiprocessing import Process

    parser = argparse.ArgumentParser(description="PGMQ Benchmarking")
    parser.add_argument(
        "--duration_seconds", type=int, required=True, help="how long the benchmark should run, in seconds"
    )
    parser.add_argument("--tps", type=int, default=400, help="number of messages to produce per second")
    parser.add_argument(
        "--agg_window", type=int, default=10_000, help="number of messages to aggregate for rolling average"
    )
    parser.add_argument("--partition_interval", type=int, default=10_000, help="number of messages per partition")
    parser.add_argument("--message_retention", type=int, default=1_000_000, help="number of messages per partition")
    parser.add_argument("--read_concurrency", type=int, default=1, help="number of concurrent consumers")
    parser.add_argument("--bench_name", type=str, required=False, help="the name of the benchmark")

    args = parser.parse_args()
    print(args)

    duration_seconds = args.duration_seconds
    tps = args.tps
    agg_window = args.agg_window
    partition_interval = args.partition_interval
    retention_interval = args.message_retention
    bench_name = args.bench_name

    if bench_name is None:
        bench_name = random.randint(0, 1000)

    test_queue = f"bench_queue_{bench_name}"
    connection_info = dict(host="localhost", port=28815, username="postgres", password="postgres", database="postgres")
    queue = PGMQueue(**connection_info)  # type: ignore
    print(f"Creating queue: {test_queue}")

    queue.create_partitioned_queue(
        test_queue, partition_interval=partition_interval, retention_interval=retention_interval
    )

    produce_csv = f"produce_{test_queue}.csv"
    consume_csv = f"consume_{test_queue}.csv"

    # run producing and consuming in parallel, separate processes

    proc_produce = Process(target=produce, args=(test_queue, connection_info, duration_seconds, tps))
    proc_produce.start()

    consume_procs = {}
    for i in range(args.read_concurrency):
        conumser = f"consumer_{i}"
        consume_procs[conumser] = Process(target=consume, args=(test_queue, connection_info))
        consume_procs[conumser].start()

    for consumer, proc in consume_procs.items():
        print(f"Waiting for {consumer} to finish")
        proc.join()
        print(f"{consumer} finished")

    # once consuming finishes, summarize
    results_file = f"results_{test_queue}.jpg"
    # TODO: organize results in a directory or something, log all the params
    filename = summarize(test_queue, queue, results_file=results_file, duration_seconds=duration_seconds, tps=tps)

    params = {
        "duration_seconds": duration_seconds,
        "tps": tps,
        "partition_interval": partition_interval,
        "retention_interval": retention_interval,
        "read_concurrency": args.read_concurrency,
        "bench_name": bench_name,
        "agg_window": agg_window,
    }

    generate_plot(filename, bench_name, duration_seconds, tps, window=agg_window, params=params)
