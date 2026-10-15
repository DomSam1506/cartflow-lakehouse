"""Run the same question against RAW and CLEAN Athena tables; record bytes scanned and engine time.

    python scripts/athena_bench.py            # writes docs/benchmark.md

Metrics come from Athena itself (get_query_execution), not from a stopwatch.
"""
import statistics
import time
from pathlib import Path

import boto3

WORKGROUP = "cartflow"
RUNS = 3
DAY = "2017-11-24"
WINDOW = ("2017-11-20", "2017-12-01")

# (label, raw_sql, clean_sql). Raw columns are strings, so raw needs CASTs; clean is already typed.
PAIRS = [
    ("1. One day: events by type (Black Friday)",
     f"SELECT event_type, count(*) n FROM cartflow.raw_events WHERE dt = '{DAY}' GROUP BY 1 ORDER BY 1",
     f"SELECT event_type, count(*) n FROM cartflow.clean_events WHERE ingest_dt = '{DAY}' GROUP BY 1 ORDER BY 1"),
    ("2. Whole window: funnel (distinct sessions per stage)",
     "SELECT event_type, count(DISTINCT session_id) s FROM cartflow.raw_events GROUP BY 1 ORDER BY 1",
     "SELECT event_type, count(DISTINCT session_id) s FROM cartflow.clean_events GROUP BY 1 ORDER BY 1"),
    ("3. Whole window: events per day, mobile only",
     "SELECT dt, count(*) n FROM cartflow.raw_events WHERE device = 'mobile' GROUP BY 1 ORDER BY 1",
     "SELECT ingest_dt AS dt, count(*) n FROM cartflow.clean_events WHERE device = 'mobile' GROUP BY 1 ORDER BY 1"),
    ("4. Whole window: revenue by order status (orders x items join)",
     "SELECT o.order_status, count(DISTINCT o.order_id) orders, round(sum(CAST(i.price AS double)), 2) revenue "
     "FROM cartflow.raw_orders o JOIN cartflow.raw_order_items i ON i.order_id = o.order_id "
     "GROUP BY 1 ORDER BY 1",
     "SELECT o.order_status, count(DISTINCT o.order_id) orders, round(sum(i.price), 2) revenue "
     "FROM cartflow.clean_orders o JOIN cartflow.clean_order_items i ON i.order_id = o.order_id "
     "GROUP BY 1 ORDER BY 1"),
]

athena = boto3.client("athena")


def run(sql):
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise SystemExit(f"{state}: {q['Status'].get('StateChangeReason')}\n{sql}")
    st = q["Statistics"]
    return st["DataScannedInBytes"], st["EngineExecutionTimeInMillis"]


def measure(sql):
    runs = [run(sql) for _ in range(RUNS)]
    # Athena may cache nothing here (result reuse is off by default), so bytes should be identical.
    return runs[0][0], statistics.median(t for _, t in runs)


def mb(b):
    return f"{b / 1e6:,.2f} MB"


def main():
    lines = [
        "# Athena benchmark: raw JSON/CSV vs clean Parquet",
        "",
        f"Window {WINDOW[0]}..{WINDOW[1]}, workgroup `{WORKGROUP}`, median of {RUNS} runs. "
        "Numbers are Athena's own `DataScannedInBytes` and `EngineExecutionTimeInMillis`.",
        "",
        "| Query | Raw scanned | Clean scanned | Reduction | Raw ms | Clean ms |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, raw_sql, clean_sql in PAIRS:
        rb, rt = measure(raw_sql)
        cb, ct = measure(clean_sql)
        red = 100 * (1 - cb / rb) if rb else 0
        print(f"{label}: raw {mb(rb)} -> clean {mb(cb)} ({red:.0f}% less), {rt:.0f} ms -> {ct:.0f} ms")
        lines.append(f"| {label} | {mb(rb)} | {mb(cb)} | {red:.0f}% | {rt:.0f} | {ct:.0f} |")
    lines += [
        "",
        "## How to read this honestly",
        "",
        "- The gap mixes three effects: **compression/format** (gzip text vs Parquet), **columnar reads** "
        "(only the referenced columns are scanned) and **typing** (raw needs CAST, clean does not). "
        "This test does not separate them.",
        "- Raw tables include rows the clean job quarantined, so result counts differ slightly "
        "(2,979 events were rejected). That is the point of the quality gate, not a bug.",
        "- Athena bills 10 MB minimum per query; at this data size (a few MB) absolute cost is about zero. "
        "The ratio is what scales: at 1 TB the same ratio is the difference in the bill.",
        "- Engine time on tiny data is dominated by fixed overhead; do not quote it as a speedup.",
    ]
    out = Path("docs/benchmark.md")
    out.parent.mkdir(exist_ok=True)
    out.write_text("\n".join(lines) + "\n")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()