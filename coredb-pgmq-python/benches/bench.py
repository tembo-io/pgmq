import random
import time

import numpy as np

from coredb_pgmq_python import Message, PGMQueue


def bench(bench_name: str, port: str, username: str) -> dict:
    rnd = random.randint(0, 100)
    test_queue = f"bench_queue_{rnd}"
    test_message = {"hello": "world"}

    queue = PGMQueue(host="localhost", port=port, username=username, password="postgres", database="postgres")
    try:
        queue.create_queue(test_queue)
    except Exception as e:
        print("table exists?")

    num_iters = 1000

    bench_0_start = time.time()
    vt = 30

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
    write_results = summarize("writes", writes)

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
    read_results = summarize("reads", reads)

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
    delete_results = summarize("deletes", deletes)

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
    summarize("writes", writes)

    archives = []
    archive_start = time.time()
    print("Archiving Messages")
    for x in range(num_iters):
        start = time.time()
        queue.archive(test_queue, x)
        archives.append(time.time() - start)
    total_archive_time = time.time() - archive_start
    print(f"total archive time: {total_delete_time}")
    archive_results = summarize("archives", reads)

    results = {"bench_name": bench_name}
    results.update(write_results)
    results.update(read_results)
    results.update(delete_results)
    results.update(archive_results)

    return results


def summarize(cat: str, timings: list[float]) -> None:
    total = len(timings)
    mean = round(np.mean(timings), 4)
    stdev = round(np.std(timings), 4)
    _min = round(np.min(timings), 4)
    _max = round(np.max(timings), 4)
    print(f"Summary: {cat}")
    print(f"Count: {total}, mean: {mean}, stdev: {stdev}, min: {_min}, max: {_max}")
    return {
        # f"{cat}_count": total,
        f"{cat}_mean": mean,
        f"{cat}_stdev": stdev,
        f"{cat}_min": _min,
        f"{cat}_max": _max
    }
if __name__ == "__main__":
    trials = [
        ("docker", 5432, "postgres"),   # docker
        ("native", 28815, "username")  # pgrx 
    ]
    all_results = []
    for t in trials:
        all_results.append(
            bench(t[0], t[1], t[2])
        )
    print(all_results)