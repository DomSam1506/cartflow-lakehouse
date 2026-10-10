from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

S, I, D, T = StringType(), IntegerType(), DoubleType(), TimestampType()


def _schema(*cols):
    return StructType([StructField(n, t, True) for n, t in cols])


SCHEMAS = {
    "orders": _schema(
        ("order_id", S), ("customer_id", S), ("order_status", S),
        ("order_purchase_timestamp", T), ("order_approved_at", T),
        ("order_delivered_carrier_date", T), ("order_delivered_customer_date", T),
        ("order_estimated_delivery_date", T)),
    "order_items": _schema(
        ("order_id", S), ("order_item_id", I), ("product_id", S), ("seller_id", S),
        ("shipping_limit_date", T), ("price", D), ("freight_value", D)),
    "order_payments": _schema(
        ("order_id", S), ("payment_sequential", I), ("payment_type", S),
        ("payment_installments", I), ("payment_value", D)),
    "order_reviews": _schema(
        ("review_id", S), ("order_id", S), ("review_score", I),
        ("review_comment_title", S), ("review_comment_message", S),
        ("review_creation_date", T), ("review_answer_timestamp", T)),
    "customer_snapshots": _schema(
        ("customer_unique_id", S), ("zip", S), ("city", S), ("state", S),
        ("effective_date", S)),
    "products": _schema(
        ("product_id", S), ("product_category_name", S),
        ("product_name_lenght", I), ("product_description_lenght", I),
        ("product_photos_qty", I), ("product_weight_g", I), ("product_length_cm", I),
        ("product_height_cm", I), ("product_width_cm", I)),
    "sellers": _schema(
        ("seller_id", S), ("seller_zip_code_prefix", S), ("seller_city", S),
        ("seller_state", S)),
    "product_category_name_translation": _schema(
        ("product_category_name", S), ("product_category_name_english", S)),
}

# Events: event_ts stays a STRING here. We parse it ourselves (transforms.parse_ts) so a bad
# timestamp gets its own precise reason instead of the whole row becoming "corrupt".
EVENTS_SCHEMA = StructType([
    StructField("event_id", S, True), StructField("session_id", S, True),
    StructField("customer_unique_id", S, True), StructField("product_id", S, True),
    StructField("event_type", S, True), StructField("event_ts", S, True),
    StructField("device", S, True),
    StructField("_corrupt_record", S, True),
])


def read_table(spark: SparkSession, name: str, path: str) -> DataFrame:
    """Read one raw table (a file or folder) with its declared schema."""
    if name == "events":
        return (spark.read.schema(EVENTS_SCHEMA)
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(path)
                .cache())
    return (spark.read.schema(SCHEMAS[name])
            .option("header", True)
            .option("multiLine", True)   # review comments contain newlines
            .option("escape", '"')       # and quotes
            .option("mode", "PERMISSIVE")
            .csv(path))