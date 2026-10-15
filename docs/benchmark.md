# Athena benchmark: raw JSON/CSV vs clean Parquet

Window 2017-11-20..2017-12-01, workgroup `cartflow`, median of 3 runs. Numbers are Athena's own `DataScannedInBytes` and `EngineExecutionTimeInMillis`.

| Query | Raw scanned | Clean scanned | Reduction | Raw ms | Clean ms |
|---|---:|---:|---:|---:|---:|
| 1. One day: events by type (Black Friday) | 5.07 MB | 0.01 MB | 100% | 722 | 781 |
| 2. Whole window: funnel (distinct sessions per stage) | 19.22 MB | 6.84 MB | 64% | 938 | 696 |
| 3. Whole window: events per day, mobile only | 19.22 MB | 0.06 MB | 100% | 903 | 499 |
| 4. Whole window: revenue by order status (orders x items join) | 0.55 MB | 0.32 MB | 42% | 825 | 1169 |

## How to read this honestly

- The gap mixes three effects: **compression/format** (gzip text vs Parquet), **columnar reads** (only the referenced columns are scanned) and **typing** (raw needs CAST, clean does not). This test does not separate them.
- Raw tables include rows the clean job quarantined, so result counts differ slightly (2,979 events were rejected). That is the point of the quality gate, not a bug.
- Athena bills 10 MB minimum per query; at this data size (a few MB) absolute cost is about zero. The ratio is what scales: at 1 TB the same ratio is the difference in the bill.
- Engine time on tiny data is dominated by fixed overhead; do not quote it as a speedup.
