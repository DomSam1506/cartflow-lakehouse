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

## AWS deployment (event-driven ingestion)

### Flow

```
replay_s3.sh / replay.py --target s3
        |  uploads dt=YYYY-MM-DD/<table>/part-*, then _READY LAST
        v
 S3 raw bucket  --(ObjectCreated, suffix _READY)-->  Lambda cartflow-trigger
                                                         |  parse key, start run with --dt
                                                         v
                                              Glue job cartflow-clean-daily
                                    (Glue 5.0, G.1X x 2, 10 min timeout, 0 retries, 1 concurrent)
                          reads raw  -> schema + DQ rules -> UTC conversion
                          writes clean/ (Parquet, ingest_dt) and quarantine/ (with dq_reason)
                          advances watermark ONLY after both writes succeed
                                    |                                  |
                                    v                                  v
                         S3 clean + quarantine                 DynamoDB cartflow-watermarks
                                    |
                                    v
                    Athena (workgroup cartflow, database cartflow)
                    external tables, partition projection on ingest_dt
```

### Components

| Piece | Choice | Notes |
|---|---|---|
| Storage | 6 S3 buckets: raw, clean, gold, quarantine, scripts, athena-results | Created with `for_each` in Terraform |
| Trigger | S3 notification, `filter_suffix = _READY` | `aws_lambda_permission` created before the notification |
| Lambda | python3.12, `cartflow-trigger` | URL-decodes the key (`=` arrives as `%3D`), matches `^dt=YYYY-MM-DD/_READY$`, calls `start_job_run(--dt)` |
| Compute | Glue job `cartflow-clean-daily` | Code is `glue_jobs/`, helper modules shipped as a zip via `--extra-py-files` |
| State | DynamoDB `cartflow-watermarks` | One row per table, moves forward only |
| Query | Athena external tables | DDL generated from `schema.py` by `scripts/make_athena_ddl.py`; Parquet timestamps written as `TIMESTAMP_MILLIS` |
| Storage code | `glue_jobs/common/storage.py` | Same API for local paths and `s3://` URIs, so one job runs in both places |

### IAM (least privilege)

- **Glue role:** read raw and scripts; read, write and delete in clean and quarantine; read/write the one DynamoDB table; CloudWatch logs. Delete is needed for the empty-partition cleanup.
- **Lambda role:** `glue:StartJobRun` on this one job, plus logs.

### Key decisions

- **Marker-triggered, not file-triggered:** a day is many files, so one `_READY` written last means one run on a complete day.
- **One concurrent run, replay sequentially:** simpler watermark handling and a hard cost cap. `replay_day` waits for the run to finish.
- **Idempotent job:** retried or duplicate triggers are harmless. Dynamic partition overwrite replaces only that `ingest_dt`; the watermark moves last.
- **Partition projection instead of crawlers:** no crawler cost, new days are queryable immediately, and the schema has a single source of truth.
- **Local-first:** the same code was tested locally (19 tests) before any AWS spend.

### Results (verified)

| Check | Result |
|---|---|
| Local vs Athena, clean events | 197,639 = 197,639 |
| Local vs Athena, quarantined events | 2,979 = 2,979 |
| Re-run of 2017-11-20 on AWS | clean 10,155 and quarantine 157, unchanged |
| Watermarks | 9 rows in DynamoDB |
| Glue runs | 14 of 14 SUCCEEDED, 76 to 123 s each |
| Cost of the replay | 2,781 DPU-seconds, about 0.77 DPU-hours, about $0.34 at about $0.44 per DPU-hour |

Quarantine by reason: duplicate event_id 1,974; bad event_type 266; null/bad event_ts 259; null event_id 238; event_ts out of range 235; combined reasons 7.

### Known limits / next steps

- Local Terraform state and an IAM user key (single developer). Production: remote state with locking, role-based CI.
- No alarm or dead letter queue on failed runs.
- Foreign-key checks are deferred to gold, because the static dimension tables ship only on day 1.
- Gold layer (star schema, SCD2, MERGE) starts on Day 5.