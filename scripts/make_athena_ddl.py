"""Generate Athena DDL (partition projection, no crawlers) from the Spark schemas.

python scripts/make_athena_ddl.py --clean-bucket <clean> --quarantine-bucket <quarantine>
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1])
)  # repo root, so glue_jobs imports work

from glue_jobs.common.dq import load_rules
from glue_jobs.common.schema import EVENTS_SCHEMA, SCHEMAS

START, END = (
    "2017-11-01",
    "2017-12-31",
)  # projection range: widen if you replay other dates


def columns(table, quarantine, has_rules):
    schema = EVENTS_SCHEMA if table == "events" else SCHEMAS[table]
    cols = []
    for f in schema.fields:
        if f.name == "_corrupt_record":
            continue
        # events: event_ts is read as a string but written to clean as a timestamp
        kind = (
            "timestamp"
            if (table == "events" and f.name == "event_ts")
            else f.dataType.simpleString()
        )
        cols.append((f.name, kind))
    if quarantine and table == "events":
        cols += [("event_ts_raw", "string"), ("_corrupt_record", "string")]
    if has_rules:
        cols.append(("dq_warn", "string"))
        if quarantine:
            cols.append(("dq_reason", "string"))
    return cols


def ddl(db, name, cols, bucket, folder):
    body = ",\n".join(f"  `{n}` {t}" for n, t in cols)
    loc = f"s3://{bucket}/{folder}/"
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{name} (\n{body})\n"
        f"PARTITIONED BY (ingest_dt string)\n"
        f"STORED AS PARQUET\n"
        f"LOCATION '{loc}'\n"
        f"TBLPROPERTIES (\n"
        f"  'projection.enabled'='true',\n"
        f"  'projection.ingest_dt.type'='date',\n"
        f"  'projection.ingest_dt.range'='{START},{END}',\n"
        f"  'projection.ingest_dt.format'='yyyy-MM-dd',\n"
        f"  'storage.location.template'='{loc}ingest_dt=${{ingest_dt}}/')\n"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-bucket", required=True)
    ap.add_argument("--quarantine-bucket", required=True)
    ap.add_argument("--db", default="cartflow")
    ap.add_argument("--rules", default="dq/rules.yaml")
    ap.add_argument("--out", default="sql/athena")
    a = ap.parse_args()

    rules = load_rules(a.rules)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    for table in ["events", *SCHEMAS]:
        has_rules = table in rules
        (out / f"clean_{table}.sql").write_text(
            ddl(
                a.db,
                f"clean_{table}",
                columns(table, False, has_rules),
                a.clean_bucket,
                table,
            )
        )
        if has_rules:  # only tables with rules can have quarantined rows
            (out / f"quarantine_{table}.sql").write_text(
                ddl(
                    a.db,
                    f"quarantine_{table}",
                    columns(table, True, True),
                    a.quarantine_bucket,
                    table,
                )
            )
        print("wrote", table)


if __name__ == "__main__":
    main()
