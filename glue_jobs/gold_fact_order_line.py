import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


REQUIRED_ARGS = [
    "JOB_NAME",
    "ORDER_ITEMS_SILVER_BASE_PATH",
    "ORDER_ITEM_OPTIONS_SILVER_BASE_PATH",
    "GOLD_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/gold_fact_order_line.local.json")


def running_in_glue() -> bool:
    return "--JOB_NAME" in sys.argv


def default_ingest_date() -> str:
    """
    Default to the current UTC date when INGEST_DATE is not provided.
    This supports normal daily workflow runs while still allowing explicit
    backfill/debug dates.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_glue_args(required_args: list[str], optional_args: list[str]) -> dict:
    """
    Load required Glue args and any optional Glue args that are actually present.

    Glue's getResolvedOptions raises an error if an argument is requested but
    was not supplied, so optional args must be detected before requesting them.
    """
    args = getResolvedOptions(sys.argv, required_args)

    supplied_optional_args = [
        arg for arg in optional_args
        if f"--{arg}" in sys.argv
    ]

    if supplied_optional_args:
        args.update(getResolvedOptions(sys.argv, supplied_optional_args))

    return args


def load_args() -> dict:
    """
    Load arguments from AWS Glue job parameters when running in Glue.
    Load arguments from local JSON config when running locally.

    INGEST_DATE is optional in both modes:
    - If provided, use it.
    - If omitted, default to current UTC date.

    Note: local execution still requires a compatible Spark/Glue runtime.
    The local config is primarily useful for documentation and parameter tracking.
    """
    if running_in_glue():
        args = get_glue_args(REQUIRED_ARGS, OPTIONAL_ARGS)
    else:
        if not LOCAL_CONFIG_PATH.exists():
            raise FileNotFoundError(
                f"Local config file not found: {LOCAL_CONFIG_PATH}. "
                "Copy config/gold_fact_order_line.local.example.json to "
                "config/gold_fact_order_line.local.json and fill in the values."
            )

        with LOCAL_CONFIG_PATH.open("r", encoding="utf-8") as f:
            args = json.load(f)

    missing = [key for key in REQUIRED_ARGS if key not in args or not args[key]]

    if missing:
        raise ValueError(f"Missing required config values: {missing}")

    args["INGEST_DATE"] = args.get("INGEST_DATE") or default_ingest_date()

    return args


def main() -> None:
    args = load_args()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    ingest_date = args["INGEST_DATE"]

    order_items_path = (
        f'{args["ORDER_ITEMS_SILVER_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    order_options_path = (
        f'{args["ORDER_ITEM_OPTIONS_SILVER_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    gold_output_path = (
        f'{args["GOLD_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading silver order_items from: {order_items_path}")
    print(f"Reading silver order_item_options from: {order_options_path}")
    print(f"Writing gold fact_order_line to: {gold_output_path}")

    order_items = spark.read.parquet(order_items_path)
    order_options = spark.read.parquet(order_options_path)

    order_items_count = order_items.count()
    order_options_count = order_options.count()

    print(f"Silver order_items row count: {order_items_count}")
    print(f"Silver order_item_options row count: {order_options_count}")

    # Aggregate options to the line-item grain before joining.
    # This prevents duplicate line-item revenue when a line has multiple options.
    options_by_lineitem = (
        order_options
        .groupBy("order_id", "lineitem_id")
        .agg(
            F.sum(F.col("option_revenue")).cast(DecimalType(14, 2)).alias("option_revenue"),
            F.sum(F.col("discount_amount")).cast(DecimalType(14, 2)).alias("discount_amount"),
            F.count(F.lit(1)).alias("option_row_count"),
            F.max(F.col("is_discount").cast("int")).alias("has_discount_int"),
        )
        .withColumn(
            "has_options",
            F.col("option_row_count") > F.lit(0),
        )
        .withColumn(
            "has_discount",
            F.col("has_discount_int") == F.lit(1),
        )
        .drop("has_discount_int")
    )

    options_by_lineitem_count = options_by_lineitem.count()
    print(f"Aggregated option line-item row count: {options_by_lineitem_count}")

    # order_item_options has repeated candidate option keys.
    # The Gold fact_order_line job aggregates all options to order_id + lineitem_id before joining to order_items, preventing join-driven revenue duplication.
    # The pipeline does not currently deduplicate repeated option rows because repeated modifiers may represent valid source-system behavior. 
    # Further exact-duplicate analysis is recommended before applying deduplication logic.

    fact_order_line = (
        order_items.alias("oi")
        .join(
            options_by_lineitem.alias("opt"),
            on=["order_id", "lineitem_id"],
            how="left",
        )
        .select(
            F.col("oi.order_id"),
            F.col("oi.lineitem_id"),
            F.col("oi.app_name"),
            F.col("oi.restaurant_id"),
            F.col("oi.creation_time_utc"),
            F.col("oi.order_date"),
            F.col("oi.user_id"),
            F.col("oi.printed_card_number"),
            F.col("oi.is_loyalty"),
            F.col("oi.currency"),
            F.col("oi.item_category"),
            F.col("oi.item_name"),
            F.col("oi.item_price"),
            F.col("oi.item_quantity"),
            F.col("oi.gross_item_revenue"),
            F.coalesce(
                F.col("opt.option_revenue"),
                F.lit(0).cast(DecimalType(14, 2)),
            ).alias("option_revenue"),
            F.coalesce(
                F.col("opt.discount_amount"),
                F.lit(0).cast(DecimalType(14, 2)),
            ).alias("discount_amount"),
            F.coalesce(F.col("opt.option_row_count"), F.lit(0)).alias("option_row_count"),
            F.coalesce(F.col("opt.has_options"), F.lit(False)).alias("has_options"),
            F.coalesce(F.col("opt.has_discount"), F.lit(False)).alias("has_discount"),
            F.col("oi.is_valid_order_item_key"),
            F.col("oi.has_user_id"),
            F.lit(ingest_date).alias("ingest_date"),
        )
        .withColumn(
            "net_line_revenue",
            (
                F.col("gross_item_revenue")
                + F.col("option_revenue")
            ).cast(DecimalType(14, 2)),
        )
    )

    fact_count = fact_order_line.count()

    line_items_with_options_count = (
        fact_order_line
        .filter(F.col("has_options"))
        .count()
    )

    line_items_with_discount_count = (
        fact_order_line
        .filter(F.col("has_discount"))
        .count()
    )

    unmatched_option_lineitem_count = (
        options_by_lineitem.alias("opt")
        .join(
            order_items.alias("oi"),
            on=["order_id", "lineitem_id"],
            how="left_anti",
        )
        .count()
    )

    total_gross_item_revenue = (
        fact_order_line
        .select(F.sum("gross_item_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_option_revenue = (
        fact_order_line
        .select(F.sum("option_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_discount_amount = (
        fact_order_line
        .select(F.sum("discount_amount").alias("total"))
        .collect()[0]["total"]
    )

    total_net_line_revenue = (
        fact_order_line
        .select(F.sum("net_line_revenue").alias("total"))
        .collect()[0]["total"]
    )

    print(f"Gold fact_order_line row count: {fact_count}")
    print(f"Line items with options: {line_items_with_options_count}")
    print(f"Line items with discounts: {line_items_with_discount_count}")
    print(f"Unmatched option line-item keys: {unmatched_option_lineitem_count}")
    print(f"Total gross item revenue: {total_gross_item_revenue}")
    print(f"Total option revenue: {total_option_revenue}")
    print(f"Total discount amount: {total_discount_amount}")
    print(f"Total net line revenue: {total_net_line_revenue}")

    if fact_count != order_items_count:
        raise ValueError(
            "Row count mismatch: fact_order_line should preserve order_items grain. "
            f"order_items_count={order_items_count}, fact_count={fact_count}"
        )

    fact_order_line_clean_to_write = fact_order_line.drop("ingest_date")

    (
        fact_order_line_clean_to_write
        .write
        .mode("overwrite")
        .parquet(gold_output_path)
    )

    print(f"Successfully wrote gold fact_order_line to: {gold_output_path}")

    job.commit()


if __name__ == "__main__":
    main()