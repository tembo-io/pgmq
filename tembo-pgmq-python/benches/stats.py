import json
import logging

import pandas as pd
from matplotlib import pyplot as plt  # type: ignore
from scipy.ndimage import gaussian_filter1d
from sqlalchemy import create_engine, text

from tembo_pgmq_python import PGMQueue


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

    # result df is rendered in table on top of plot
    result = df.groupby("operation").agg({"time": lambda x: x.max() - x.min(), "batch_size": "sum"})
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
    color_map = {"read": "orange", "write": "blue", "archive": "green", "delete": "green", "select1": "red"}
    sigma = 1000  # Adjust as needed for the desired smoothing level
    for op in ["read", "write", "archive", "delete", "select1"]:
        _df = df[df["operation"] == op].sort_values("time")
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
