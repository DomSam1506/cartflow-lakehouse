# Architecture (clean zone)

Flow: raw/dt=DAY -> clean_daily.py --dt DAY -> clean/<table>/ingest_dt=DAY
                                             -> quarantine/<table>/ingest_dt=DAY (with dq_reason)

Decisions
- One job processes all tables for one day.
- Pure functions in glue_jobs/common/ so logic is unit-testable without AWS.
- Clean zone is partitioned by ingest_dt (replay day), not event date. A late event
  must not overwrite an earlier day's partition (dynamic partition overwrite).
  Cross-day duplicates are resolved in the gold layer.
- Writes are idempotent: re-running a day replaces only that day's partition.
- The watermark advances only after a successful write.
- Timezones: Olist timestamps are Brazil local time (America/Sao_Paulo); generated events
  copy them, so they are also Brazil local. Everything is converted to UTC in clean.
  Spark session timezone is set to UTC.
- Quality rules are config (dq/rules.yaml). Severity: reject -> quarantine, warn -> keep and log.