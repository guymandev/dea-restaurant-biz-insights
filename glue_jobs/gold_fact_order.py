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
    "FACT_ORDER_LINE_BASE_PATH",
    "GOLD_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/gold_fact_order.local.json")


def running_in_glue() -> bool:
    return "--JOB_NAME" in sys.argv


def default_ingest_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_glue_args(required_args: list[str], optional_args: list[str]) -> dict:
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
    """
    if running_in_glue():
        args = get_glue_args(REQUIRED_ARGS, OPTIONAL_ARGS)
    else:
        if not LOCAL_CONFIG_PATH.exists():
            raise FileNotFoundError(
                f"Local config file not found: {LOCAL_CONFIG_PATH}. "
                "Copy config/gold_fact_order.local.example.json to "
                "config/gold_fact_order.local.json and fill in the values."
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

    fact_order_line_path = (
        f'{args["FACT_ORDER_LINE_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    gold_output_path = (
        f'{args["GOLD_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading gold fact_order_line from: {fact_order_line_path}")
    print(f"Writing gold fact_order to: {gold_output_path}")

    fact_order_line = spark.read.parquet(fact_order_line_path)
    line_count = fact_order_line.count()

    print(f"fact_order_line row count: {line_count}")
    print(f"fact_order_line columns: {fact_order_line.columns}")

    fact_order = (
        fact_order_line
        .groupBy("order_id")
        .agg(
            F.first("app_name", ignorenulls=True).alias("app_name"),
            F.first("restaurant_id", ignorenulls=True).alias("restaurant_id"),
            F.min("creation_time_utc").alias("order_created_at_utc"),
            F.first("order_date", ignorenulls=True).alias("order_date"),
            F.first("user_id", ignorenulls=True).alias("user_id"),
            F.first("printed_card_number", ignorenulls=True).alias("printed_card_number"),
            F.max(F.col("is_loyalty").cast("int")).alias("is_loyalty_int"),
            F.first("currency", ignorenulls=True).alias("currency"),

            F.count(F.lit(1)).alias("line_item_count"),
            F.countDistinct("lineitem_id").alias("distinct_line_item_count"),

            F.sum("item_quantity").alias("total_item_quantity"),
            F.sum("gross_item_revenue").cast(DecimalType(14, 2)).alias("gross_item_revenue"),
            F.sum("option_revenue").cast(DecimalType(14, 2)).alias("option_revenue"),
            F.sum("discount_amount").cast(DecimalType(14, 2)).alias("discount_amount"),
            F.sum("net_line_revenue").cast(DecimalType(14, 2)).alias("net_order_revenue"),

            F.sum("option_row_count").alias("option_row_count"),
            F.max(F.col("has_options").cast("int")).alias("has_options_int"),
            F.max(F.col("has_discount").cast("int")).alias("has_discount_int"),
            F.max(F.col("has_user_id").cast("int")).alias("has_user_id_int"),
            F.min(F.col("is_valid_order_item_key").cast("int")).alias("all_line_keys_valid_int"),
        )
        .withColumn("is_loyalty", F.col("is_loyalty_int") == F.lit(1))
        .withColumn("has_options", F.col("has_options_int") == F.lit(1))
        .withColumn("has_discount", F.col("has_discount_int") == F.lit(1))
        .withColumn("has_user_id", F.col("has_user_id_int") == F.lit(1))
        .withColumn("all_line_keys_valid", F.col("all_line_keys_valid_int") == F.lit(1))
        .withColumn("ingest_date", F.lit(ingest_date))
        .drop(
            "is_loyalty_int",
            "has_options_int",
            "has_discount_int",
            "has_user_id_int",
            "all_line_keys_valid_int",
        )
    )

    order_count = fact_order.count()

    orders_with_user_id_count = fact_order.filter(F.col("has_user_id")).count()
    orders_with_options_count = fact_order.filter(F.col("has_options")).count()
    orders_with_discount_count = fact_order.filter(F.col("has_discount")).count()
    invalid_order_key_count = fact_order.filter(~F.col("all_line_keys_valid")).count()

    total_gross_item_revenue = (
        fact_order
        .select(F.sum("gross_item_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_option_revenue = (
        fact_order
        .select(F.sum("option_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_discount_amount = (
        fact_order
        .select(F.sum("discount_amount").alias("total"))
        .collect()[0]["total"]
    )

    total_net_order_revenue = (
        fact_order
        .select(F.sum("net_order_revenue").alias("total"))
        .collect()[0]["total"]
    )

    print(f"Gold fact_order row count: {order_count}")
    print(f"Orders with user_id: {orders_with_user_id_count}")
    print(f"Orders with options: {orders_with_options_count}")
    print(f"Orders with discounts: {orders_with_discount_count}")
    print(f"Orders with invalid line keys: {invalid_order_key_count}")
    print(f"Total gross item revenue: {total_gross_item_revenue}")
    print(f"Total option revenue: {total_option_revenue}")
    print(f"Total discount amount: {total_discount_amount}")
    print(f"Total net order revenue: {total_net_order_revenue}")

    if order_count <= 0:
        raise ValueError("fact_order produced zero rows.")

    duplicate_order_id_count = (
        fact_order
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_order_id_count > 0:
        raise ValueError(
            f"fact_order should have one row per order_id, but found "
            f"{duplicate_order_id_count} duplicate order_id groups."
        )

    (
        fact_order
        .write
        .mode("overwrite")
        .parquet(gold_output_path)
    )

    print(f"Successfully wrote gold fact_order to: {gold_output_path}")

    job.commit()


if __name__ == "__main__":
    main()