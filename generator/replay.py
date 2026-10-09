# generator/replay.py
"""Ship one day of staged data into the raw zone, then drop the _READY marker LAST.

    python generator/replay.py --day 2017-11-24 --target local
    python generator/replay.py --day 2017-11-24 --target s3 --bucket cartflow-xxxx-raw
    python generator/replay.py --all --target local          # every day in staging, in order

Layout produced (local: data/local_lake/raw/, s3: bucket root + --prefix):
    dt=2017-11-24/orders/part-0.csv.gz
    dt=2017-11-24/events/part-0.json.gz
    dt=2017-11-24/_READY
"""
import argparse
import shutil
from pathlib import Path


def day_files(day, tables_dir, events_dir):
    """Yield (relative_key, local_path) for everything staged for this day."""
    for p in sorted((tables_dir / f"dt={day}").glob("*/*")):
        yield f"dt={day}/{p.parent.name}/{p.name}", p
    for p in sorted((events_dir / f"dt={day}").glob("*")):
        yield f"dt={day}/events/{p.name}", p


def staged_days(tables_dir, events_dir):
    days = {p.name[3:] for d in (tables_dir, events_dir) for p in d.glob("dt=*")}
    return sorted(days)


def replay(day, a):
    files = list(day_files(day, Path(a.tables), Path(a.events)))
    if not files:
        raise SystemExit(f"nothing staged for {day}; run slice_tables.py / gen_events.py first")

    if a.target == "local":
        root = Path(a.lake) / "raw"
        shutil.rmtree(root / f"dt={day}", ignore_errors=True)  # re-run = clean overwrite
        for key, path in files:
            dest = root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, dest)
        (root / f"dt={day}" / "_READY").touch()  # LAST
    else:
        import boto3  # only needed for the S3 target
        if not a.bucket:
            raise SystemExit("--bucket is required for --target s3")
        s3 = boto3.client("s3")
        for key, path in files:
            s3.upload_file(str(path), a.bucket, a.prefix + key)
        s3.put_object(Bucket=a.bucket, Key=f"{a.prefix}dt={day}/_READY", Body=b"")  # LAST
    print(f"{day}: {len(files)} files + _READY -> {a.target}")


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--day")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--target", choices=["local", "s3"], default="local")
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="", help="optional key prefix for s3, e.g. 'raw/'")
    ap.add_argument("--lake", default="data/local_lake")
    ap.add_argument("--tables", default="data/generated/tables")
    ap.add_argument("--events", default="data/generated/events")
    a = ap.parse_args()

    days = staged_days(Path(a.tables), Path(a.events)) if a.all else [a.day]
    for day in days:
        replay(day, a)


if __name__ == "__main__":
    main()