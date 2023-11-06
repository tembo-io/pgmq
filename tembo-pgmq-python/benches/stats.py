import json
import logging

import pandas as pd
from matplotlib import pyplot as plt  # type: ignore
from scipy.ndimage import gaussian_filter1d
from sqlalchemy import create_engine, text

from tembo_pgmq_python import PGMQueue


def stack_events(
    bench_name: str,
    queue: PGMQueue,
) -> pd.DataFrame:
    """stacks operation log to queue depth log into single dataframe

    each record is an event, either a write, read, delete, or a queue_depth log

    queue_depth log happen on an interval and measure number of messages in queue and `select 1` latency
    """
    db_url = f"postgresql://{queue.username}:{queue.password}@{queue.host}:{queue.port}/{queue.database}"
    con = create_engine(db_url)

    bench_results_table = f"bench_results_{bench_name}"
    df = pd.read_sql(f'''select * from "{bench_results_table}"''', con=con)

    # merge schemas of queue depth so we can plot it in-line with latency results
    queue_depth_table = f"{bench_results_table}_queue_depth"
    queue_depth = pd.read_sql(f'''select * from "{queue_depth_table}"''', con=con)

    sel1_df = queue_depth[["time", "select1"]]
    sel1_df["operation"] = "select1"
    sel1_df.rename(
        columns={
            "select1": "duration_sec",
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

    events_df = pd.concat([df, sel1_df, queue_depth[["operation", "queue_length", "epoch"]]])

    from benches.log import write_event_log

    # write event log back to postgres table
    write_event_log(db_url=db_url, event_log=events_df, bench_name=bench_name)

    # remove temp tables
    with con.connect() as c:
        c.execute(text(f'''DROP TABLE "{bench_results_table}"'''))
        c.execute(text(f'''DROP TABLE "{queue_depth_table}"'''))
        c.commit()
    return events_df


def plot_rolling(event_log: pd.DataFrame, summary_df: pd.DataFrame, bench_name: str, duration_sec: int, params: dict):
    def int_to_comma_string(n):
        return "{:,}".format(n)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(20, 10))
    plt.suptitle("PGMQ Concurrent Produce/Consumer Benchmark")
    ax1.text(0.25, -0.05, json.dumps(params, indent=2), transform=ax1.transAxes, va="top", ha="left")

    # Prepare the throughput table
    columns = ["Operation", "Duration (s)", "Total Messages", "msg/s"]
    cell_text = []
    for _, row in summary_df.iterrows():
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
    color_map = {"read": "orange", "write": "blue", "archive": "green", "delete": "green", "select1": "red"}
    sigma = 1000  # Adjust as needed for the desired smoothing level
    for op in ["read", "write", "archive", "delete", "select1"]:
        _df = event_log[event_log["operation"] == op].sort_values("time")
        if _df.shape[0] == 0:
            # skip delete when archive, and vice-versa
            continue

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
    queue_depth_data = event_log[event_log["operation"] == "queue_depth"]
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


import numpy as np


# # TODO: make return model data type
def summarize(event_log: pd.DataFrame) -> dict:
    """Compute summary stats from the bench

    event_log: pd.DataFrame representnation of event_log table
        operation text NOT NULL,
        duration_sec numeric NULL,
        msg_ids jsonb NULL,
        batch_size numeric NULL,
        epoch numeric NOT NULL,
        queue_length numeric NULL
    """
    event_log["duration_ms"] = event_log["duration_sec"] * 1000
    event_log["time"] = pd.to_datetime(event_log["epoch"], unit="s")

    # result df is rendered in table on top of plot
    result = (
        event_log.groupby("operation")
        .agg(
            {
                "time": lambda x: x.max() - x.min(),  # total duration of eacah operation
                "duration_ms": [np.mean, np.std],
                "batch_size": "sum",  # sum the total number of messages read in each operation
            }
        )
        .reset_index()
    )
    result.columns = ["operation", "range", "mean", "stddev", "num_messages"]

    result["total_duration_seconds"] = result["range"].apply(lambda x: x.total_seconds())
    result["messages_per_second"] = result["num_messages"] / result["total_duration_seconds"]
    return result.drop("range", axis=1)
