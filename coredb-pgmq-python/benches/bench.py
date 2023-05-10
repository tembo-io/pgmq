import random
import time

import numpy as np

from coredb_pgmq_python import Message, PGMQueue


def bench(host: str, port: str, username: str, num_iters: int = 1000, vt= 10) -> list[dict]:
    rnd = random.randint(0, 100)
    test_queue = f"bench_queue_{rnd}"
    test_message = {"hello": "world"}

    queue = PGMQueue(host=host, port=port, username=username, password="postgres", database="postgres")
    try:
        queue.create_queue(test_queue)
    except Exception as e:
        print("table exists?")

    bench_0_start = time.time()

    print(f"""
    Starting benchmark
    Total messages: {num_iters}
    """)

    writes = []
    total_write_start = time.time()
    # publish messages
    print("Writing Messages")
    for x in range(num_iters):
        test_message["hello"] = x
        start = time.time()
        msg_id = queue.send(test_queue, test_message)
        writes.append(time.time() - start)
    total_write_duration = time.time() - total_write_start
    print(f"total write time: {total_write_duration}")
    write_results = summarize("writes", writes, host)

    reads = []
    total_read_start = time.time()
    # read them all once, each
    print("Reading Messages")
    for x in range(num_iters):
        start = time.time()
        message: Message = queue.read(test_queue, vt = vt)
        reads.append(time.time() - start)
        if x % 100 == 0:
            print(f"read {x} messages")

    total_read_time = time.time() - total_read_start
    print(f"total read time: {total_read_time}")
    read_results = summarize("reads", reads, host)

    # wait for all VT to expire
    while time.time() - bench_0_start < vt:
        print("waiting for all VTs to expire")
        time.sleep(1)

    # deletes
    deletes = []
    delete_start = time.time()
    print("Deleting Messages")
    for x in range(num_iters):
        start = time.time()
        queue.delete(test_queue, x)
        deletes.append(time.time() - start)
    total_delete_time = time.time() - delete_start
    print(f"total delete time: {total_delete_time}")
    delete_results = summarize("deletes", deletes, host)

    # archives
    print("Benchmarking: Archiving Messages")
    writes = []
    total_write_start = time.time()
    # publish messages
    print("Writing Messages")
    for x in range(num_iters):
        test_message["hello"] = x
        start = time.time()
        msg_id = queue.send(test_queue, test_message)
        writes.append(time.time() - start)
    total_write_duration = time.time() - total_write_start
    print(f"total write time: {total_write_duration}")
    summarize("writes", writes, host)

    archives = []
    archive_start = time.time()
    print("Archiving Messages")
    for x in range(num_iters):
        start = time.time()
        queue.archive(test_queue, x)
        archives.append(time.time() - start)
    total_archive_time = time.time() - archive_start
    print(f"total archive time: {total_archive_time}")
    archive_results = summarize("archives", archives, host)

    results = []
    results.append(write_results)
    results.append(read_results)
    results.append(delete_results)
    results.append(archive_results)

    return results


def summarize(operation: str, timings: list[float], host: str) -> None:
    total = len(timings)
    mean = round(np.mean(timings), 4)
    stdev = round(np.std(timings), 4)
    _min = round(np.min(timings), 4)
    _max = round(np.max(timings), 4)
    print(f"Summary: {operation}")
    print(f"Count: {total}, mean: {mean}, stdev: {stdev}, min: {_min}, max: {_max}")
    return {
        "host": host,
        "operation": operation,
        "count": total,
        "mean": mean,
        "stdev": stdev,
        "min": _min,
        "max": _max
    }




def bench_line_item(host: str, port: str, username: str, num_iters: int = 1000, vt= 10) -> list[dict]:
    """logs each transaction as a separate line
    
    returns:
            [{
                "operation": <operation>,
                "duration": time.time() - start,
                "msg_id": msg_id
            }]
    """
    rnd = random.randint(0, 100)
    test_queue = f"bench_queue_{rnd}"
    test_message = {"hello": "world"}
    bench_0_start = time.time()

    queue = PGMQueue(host=host, port=port, username=username, password="postgres", database="postgres")
    try:
        queue.create_queue(test_queue)
    except Exception as e:
        print("table exists?")


    print(f"""
    Starting benchmark
    Total messages: {num_iters}
    """)



    total_results = []

    # publish messages
    print("Writing Messages")
    all_msg_ids = []
    for x in range(num_iters):
        test_message["hello"] = x
        start = time.time()
        msg_id = queue.send(test_queue, test_message)
        total_results.append(
            {
                "operation": "write",
                "duration": time.time() - start,
                "msg_id": msg_id
            }
        )
        all_msg_ids.append(msg_id)

    # read them all once, each
    print("Reading Messages")
    for x in range(num_iters):
        start = time.time()
        message: Message = queue.read(test_queue, vt = vt)
        if x % 100 == 0:
            print(f"read {x} messages")
        total_results.append(
            {
                "operation": "read",
                "duration": time.time() - start,
                "msg_id": message.msg_id
            }
        )

    # wait for all VT to expire
    while time.time() - bench_0_start < vt:
        print("waiting for all VTs to expire")
        time.sleep(2)

    # deletes
    print("Deleting Messages")
    for x in all_msg_ids:
        start = time.time()
        queue.delete(test_queue, x)
        total_results.append(
            {
                "operation": "delete",
                "duration": time.time() - start,
                "msg_id": x
            }
        )

    # archives
    print("Benchmarking: Archiving Messages")
    all_msg_ids = []
    # publish messages
    for x in range(num_iters):
        test_message["hello"] = x
        start = time.time()
        msg_id = queue.send(test_queue, test_message)
        all_msg_ids.append(msg_id)


    print("Archiving Messages")
    for x in all_msg_ids:
        start = time.time()
        queue.archive(test_queue, x)
        total_results.append(
            {
                "operation": "archive",
                "duration": time.time() - start,
                "msg_id": x
            }
        )

    return total_results

if __name__ == "__main__":
    n_messages = 100
    trials = [
        ("localhost", 5432, "postgres", n_messages),     # docker
        # ("local", 28815, "username")    # pgrx 
    ]
    all_results = []
    for t in trials:
        all_results.append(
            bench(t[0], t[1], t[2], t[3])
        )
    print(all_results)
