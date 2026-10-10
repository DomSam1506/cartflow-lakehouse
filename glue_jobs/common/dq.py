import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def load_rules(path: str = "dq/rules.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _flag(df: DataFrame, block: dict | None, out_col: str) -> DataFrame:
    """Add out_col: a comma-separated list of reasons, or '' if the row passes every rule.
    A NULL condition counts as 'not violated', so null handling belongs to not_null only."""
    block = block or {}
    exprs, tmp = [], []

    if block.get("corrupt_record"):
        exprs.append(F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_record")))

    for c in block.get("not_null", []):
        exprs.append(F.when(F.col(c).isNull(), F.lit(f"null:{c}")))

    for c in block.get("positive", []):
        exprs.append(F.when(F.col(c) <= 0, F.lit(f"non_positive:{c}")))

    for c, allowed in block.get("allowed_values", {}).items():
        exprs.append(F.when(F.col(c).isNotNull() & ~F.col(c).isin(allowed),
                            F.lit(f"bad_value:{c}")))

    for c in block.get("valid_timestamp", []):
        # raw text present but parsing gave NULL (needs transforms.parse_ts first)
        exprs.append(F.when(F.col(f"{c}_raw").isNotNull() & F.col(c).isNull(),
                            F.lit(f"bad_timestamp:{c}")))

    if "ts_range" in block:
        r = block["ts_range"]
        c = F.col(r["column"])
        lo, hi = F.lit(r["min"]).cast("timestamp"), F.lit(r["max"]).cast("timestamp")
        exprs.append(F.when(c.isNotNull() & ((c < lo) | (c > hi)),
                            F.lit(f"out_of_range:{r['column']}")))

    for r in block.get("required_when", []):
        exprs.append(F.when(
            (F.col(r["when_column"]) == r["equals"]) & F.col(r["column"]).isNull(),
            F.lit(f"missing_when:{r['column']}:{r['when_column']}={r['equals']}")))

    for r in block.get("date_order", []):
        e, l = r["earlier"], r["later"]
        exprs.append(F.when(F.col(e).isNotNull() & F.col(l).isNotNull() & (F.col(l) < F.col(e)),
                            F.lit(f"date_order:{l}<{e}")))

    for c in block.get("unique", []):
        # every row after the first with the same key is a duplicate
        dup = f"_dup_{c}"
        df = df.withColumn(dup, F.row_number().over(Window.partitionBy(c).orderBy("_rid")))
        exprs.append(F.when(F.col(c).isNotNull() & (F.col(dup) > 1), F.lit(f"duplicate:{c}")))
        tmp.append(dup)

    reason = F.concat_ws(",", *exprs) if exprs else F.lit("")
    return df.withColumn(out_col, reason).drop(*tmp)


def apply_rules(df: DataFrame, rules: dict):
    """Returns (valid, bad).
    valid: passed every reject rule; keeps a dq_warn column (reasons from warn rules, or '').
    bad:   failed at least one reject rule; has dq_reason (and dq_warn)."""
    df = df.withColumn("_rid", F.monotonically_increasing_id())
    df = _flag(df, rules.get("reject"), "dq_reason")
    df = _flag(df, rules.get("warn"), "dq_warn").drop("_rid").cache()
    valid = df.filter(F.col("dq_reason") == "").drop("dq_reason")
    bad = df.filter(F.col("dq_reason") != "")
    return valid, bad


def summarize(table: str, total: int, bad: DataFrame) -> dict:
    """The numbers for your README: quarantined / total, with a breakdown by reason."""
    n = bad.count()
    by_reason = {
        r["reason"]: r["count"]
        for r in (bad.select(F.explode(F.split("dq_reason", ",")).alias("reason"))
                     .groupBy("reason").count().orderBy(F.desc("count")).collect())
    }
    return {"table": table, "total": total, "quarantined": n,
            "pct": round(100.0 * n / total, 3) if total else 0.0, "by_reason": by_reason}