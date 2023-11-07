import json
import logging
import multiprocessing
import os
import time

import psycopg

logging.basicConfig(level=logging.INFO)


def produce(
    queue_name: str,
    connection_info: dict,
    duration_seconds: int = 60,
    batch_size: int = 1,
    message_size_bytes: int = 1000,
):
    """Sends minimal message to a queue for the specified with no pause in between sends"""
    batch_size = int(batch_size)
    pid = os.getpid()
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    with psycopg.connect(url, autocommit=True) as conn:
        cur = conn.cursor()

        all_results = []

        start_time = time.time()

        num_msg = 0
        running_duration = 0
        last_print_time = time.time()

        payloads = {22: "22B.json", 1000: "1KB.json", 10000: "10KB.json"}

        # get specified message payload by size
        # just fail hard if its invalid value
        message_file = payloads[message_size_bytes]

        with open(f"benches/payloads/{message_file}", "r") as f:
            message = json.dumps(json.load(f))

        if batch_size > 1:
            # create array of messages when in batch mode
            msg = [message for _ in range(batch_size)]
            message = msg

        while running_duration < duration_seconds:
            send_start = time.perf_counter()
            if batch_size > 1:
                cur.execute(
                    "select * from pgmq.send_batch(%s, %s::jsonb[])",
                    (
                        queue_name,
                        message,
                    ),
                )

            else:
                cur.execute(
                    "select * from pgmq.send(%s, %s::jsonb)",
                    (
                        queue_name,
                        message,
                    ),
                )
            send_duration = time.perf_counter() - send_start
            all_results.append(
                # {
                #     "operation": "write",
                #     "duration": send_duration,
                #     "batch_size": batch_size,
                #     "epoch": time.time(),
                # }
                ("write", send_duration, batch_size, time.time())
            )
            num_msg += 1
            running_duration = int(time.time() - start_time)
            # log every 5 seconds
            if time.time() - last_print_time >= 5:
                last_print_time = time.time()
                logging.debug(f"pid: {pid}, total_sent: {num_msg}, {running_duration} / {duration_seconds} seconds")

        logging.debug(f"pid: {pid}, total_sent: {num_msg}, {running_duration} / {duration_seconds} seconds")

        with cur.copy(f"COPY bench_results_{queue_name} FROM STDIN") as copy:
            for record in all_results:
                copy.write_row(record)
    logging.info(f"producer complete, pid: {pid}")


def consume(queue_name: str, connection_info: dict, pattern: str = "delete", batch_size: int = 1):
    """Consumes messages from a queue. Times and writes results to csv.

    Halts consumption after 5 seconds of no messages.
    """
    pid = os.getpid()
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    with psycopg.connect(url, autocommit=True) as conn:
        cur = conn.cursor()

        results = []
        no_message_timeout = 0
        while no_message_timeout < 20:
            # stmt = f"select * from pgmq.read('{queue_name}', 1, 1)"
            read_start = time.perf_counter()
            cur.execute("select * from pgmq.read(%s, 1, %s)", (queue_name, batch_size))
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

            msg_ids = [x[0] for x in message]

            num_consumed = int(len(msg_ids))

            results.append(
                # {
                #     "operation": "read",
                #     "duration_sec": read_duration,
                #     "batch_size": num_consumed,
                #     "epoch": time.time(),
                # }
                ("read", read_duration, num_consumed, time.time())
            )

            archive_start = time.perf_counter()
            if pattern == "archive":
                cur.execute("select * from pgmq.archive(%s, %s);", [queue_name, msg_ids])
            else:
                cur.execute("select * from pgmq.delete(%s, %s);", [queue_name, msg_ids])

            cur.fetchall()

            archive_duration = time.perf_counter() - archive_start
            results.append(
                # {
                #     "operation": pattern,
                #     "duration_sec": archive_duration,
                #     "batch_size": num_consumed,
                #     "epoch": time.time(),
                # }
                (pattern, archive_duration, num_consumed, time.time())
            )

            if num_consumed < batch_size:
                logging.debug(f"Consumed {num_consumed}/{batch_size} batch size")

        with cur.copy(f"COPY bench_results_{queue_name} FROM STDIN") as copy:
            for record in results:
                copy.write_row(record)
    # divide by 2 because we're appending two results (read/archive) per message
    num_consumed = len(results) / 2
    logging.info(f"pid: {pid}, read {num_consumed} messages")


def queue_depth(queue_name: str, connection_info: dict, kill_flag: multiprocessing.Value, duration_seconds: int):
    username = connection_info["username"]
    password = connection_info["password"]
    host = connection_info["host"]
    port = connection_info["port"]
    database = connection_info["database"]
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    with psycopg.connect(url, autocommit=True) as conn:
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
                # {
                #     "queue_name": metrics[0],
                #     "queue_length": depth,
                #     "total_messages": total_messages,
                #     "select1": sel_duration,
                #     "time": time.time(),
                # }
                # use a tuple to use less memory
                (metrics[0], depth, total_messages, sel_duration, time.time())
            )
            log = {
                "q_len": depth,
                "elapsed": f"{duration}/{duration_seconds}",
                "select1_sec": sel_duration,
                "tot_msg": total_messages,
            }
            logging.info(log)
            time.sleep(5)

        with cur.copy(f"COPY bench_results_{queue_name}_queue_depth FROM STDIN") as copy:
            for record in all_metrics:
                copy.write_row(record)

    return all_metrics
