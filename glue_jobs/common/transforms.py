from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dedupe(df: DataFrame, key: str) -> DataFrame:
    """Drop repeated rows that share the same key (keeps one arbitrary row per key)."""
    return df.dropDuplicates([key])


def parse_ts(df: DataFrame, col: str) -> DataFrame:
    """Parse a string timestamp column. Keeps the original text in <col>_raw so a failed
    parse can be reported ('raw present but parsed is null')."""
    return (df.withColumn(f"{col}_raw", F.col(col))
              .withColumn(col, F.to_timestamp(F.col(col))))


def to_utc(df: DataFrame, col: str, src_tz: str = "America/Sao_Paulo") -> DataFrame:
    """Treat col as wall-clock time in src_tz and convert to UTC."""
    return df.withColumn(col, F.to_utc_timestamp(F.col(col), src_tz))