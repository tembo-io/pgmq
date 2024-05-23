from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import copy
import logging
import typer
from benches.bench import bench


def run_multi(infile: str):
    """
    run a series of benchmarks sequentially, where each bench defined in yaml.

    For example:

    poetry run python -m benches.runner bench.yaml

    # bench.yaml
    globals:
        postgres_connection: 'postgresql://postgres:postgres@localhost:5432/postgres'
        duration_seconds: 60
        message_size_bytes: 1000
    benches:
    - read_concurrency: 1
        read_batch_size: 1
        write_concurrency: 1
        write_batch_size: 1
    - read_concurrency: 2
        read_batch_size: 2
        write_concurrency: 2
        write_batch_size: 2
    - read_concurrency: 3
        read_batch_size: 3
        write_concurrency: 3
        write_batch_size: 3
    """
    with open(infile, "r") as f:
        config = load(f, Loader=Loader)

    # start with global defaults
    benches: list[dict] = []
    for _b in config["benches"]:
        this_bench = copy.deepcopy(config.get("globals", {}))
        this_bench.update(_b)
        benches.append(this_bench)

    completed_benches: list[str] = []
    for i, b in enumerate(benches):
        logging.info("Starting bench %s / %s", i + 1, len(benches))
        print(b)
        bench_name = bench(**b)
        completed_benches.append(bench_name)

    logging.info(f"All benches complete: {completed_benches}")


if __name__ == "__main__":
    typer.run(run_multi)
