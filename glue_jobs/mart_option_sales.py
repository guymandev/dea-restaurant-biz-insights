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
    "ORDER_ITEM_OPTIONS_SILVER_BASE_PATH",
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_option_sales.local.json")


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
                "Copy config/mart_option_sales.local.example.json to "
                "config/mart_option_sales.local.json and fill in the values."
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

    order_item_options_path = (
        f'{args["ORDER_ITEM_OPTIONS_SILVER_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading silver order_item_options_clean from: {order_item_options_path}")
    print(f"Writing option_sales mart to: {mart_output_path}")

    option_rows = spark.read.parquet(order_item_options_path)
    source_count = option_rows.count()

    print(f"order_item_options_clean row count: {source_count}")
    print(f"order_item_options_clean columns: {option_rows.columns}")

    if source_count <= 0:
        raise ValueError("order_item_options_clean source has zero rows.")

    option_rows_prepared = (
        option_rows
        .withColumn(
            "order_line_key",
            F.concat_ws("||", F.col("order_id"), F.col("lineitem_id")),
        )
    )

    option_sales = (
        option_rows_prepared
        .groupBy("option_group_name", "option_name")
        .agg(
            F.count(F.lit(1)).alias("option_row_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("order_line_key").alias("line_item_count"),

            F.sum("option_quantity").alias("total_option_quantity"),

            F.avg("option_price").cast(DecimalType(14, 2)).alias("avg_option_price"),
            F.min("option_price").cast(DecimalType(14, 2)).alias("min_option_price"),
            F.max("option_price").cast(DecimalType(14, 2)).alias("max_option_price"),

            F.sum("option_revenue").cast(DecimalType(14, 2)).alias("option_revenue"),
            F.sum("discount_amount").cast(DecimalType(14, 2)).alias("discount_amount"),

            F.sum(F.col("is_discount").cast("int")).alias("discount_row_count"),
            F.sum(F.col("is_valid_option_key").cast("int")).alias("valid_option_key_row_count"),
        )
        .withColumn(
            "avg_option_revenue_per_row",
            (F.col("option_revenue") / F.col("option_row_count")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "avg_option_revenue_per_unit",
            (F.col("option_revenue") / F.col("total_option_quantity")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "discount_row_rate",
            (F.col("discount_row_count") / F.col("option_row_count")).cast(DecimalType(14, 4)),
        )
        .withColumn(
            "valid_option_key_rate",
            (F.col("valid_option_key_row_count") / F.col("option_row_count")).cast(DecimalType(14, 4)),
        )
        .withColumn("ingest_date", F.lit(ingest_date))
    )

    mart_count = option_sales.count()

    distinct_option_count = (
        option_sales
        .select("option_group_name", "option_name")
        .distinct()
        .count()
    )

    total_source_option_revenue = (
        option_rows
        .select(F.sum("option_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_mart_option_revenue = (
        option_sales
        .select(F.sum("option_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_source_discount_amount = (
        option_rows
        .select(F.sum("discount_amount").alias("total"))
        .collect()[0]["total"]
    )

    total_mart_discount_amount = (
        option_sales
        .select(F.sum("discount_amount").alias("total"))
        .collect()[0]["total"]
    )

    total_source_option_quantity = (
        option_rows
        .select(F.sum("option_quantity").alias("total"))
        .collect()[0]["total"]
    )

    total_mart_option_quantity = (
        option_sales
        .select(F.sum("total_option_quantity").alias("total"))
        .collect()[0]["total"]
    )

    top_option_by_revenue = (
        option_sales
        .orderBy(F.col("option_revenue").desc())
        .select("option_group_name", "option_name", "option_revenue")
        .limit(1)
        .collect()
    )

    top_option_by_quantity = (
        option_sales
        .orderBy(F.col("total_option_quantity").desc())
        .select("option_group_name", "option_name", "total_option_quantity")
        .limit(1)
        .collect()
    )

    print(f"Option sales mart row count: {mart_count}")
    print(f"Distinct option group/name combinations: {distinct_option_count}")
    print(f"Total source option revenue: {total_source_option_revenue}")
    print(f"Total option_sales option revenue: {total_mart_option_revenue}")
    print(f"Total source discount amount: {total_source_discount_amount}")
    print(f"Total option_sales discount amount: {total_mart_discount_amount}")
    print(f"Total source option quantity: {total_source_option_quantity}")
    print(f"Total option_sales option quantity: {total_mart_option_quantity}")

    if top_option_by_revenue:
        row = top_option_by_revenue[0]
        print(
            "Top option by revenue: "
            f"option_group_name={row['option_group_name']}, "
            f"option_name={row['option_name']}, "
            f"option_revenue={row['option_revenue']}"
        )

    if top_option_by_quantity:
        row = top_option_by_quantity[0]
        print(
            "Top option by quantity: "
            f"option_group_name={row['option_group_name']}, "
            f"option_name={row['option_name']}, "
            f"total_option_quantity={row['total_option_quantity']}"
        )

    if mart_count <= 0:
        raise ValueError("option_sales mart produced zero rows.")

    if mart_count != distinct_option_count:
        raise ValueError(
            "option_sales should have one row per option_group_name + option_name. "
            f"mart_count={mart_count}, "
            f"distinct_option_count={distinct_option_count}"
        )

    if total_mart_option_revenue != total_source_option_revenue:
        raise ValueError(
            "Option revenue mismatch between order_item_options_clean and option_sales mart. "
            f"source_total={total_source_option_revenue}, "
            f"mart_total={total_mart_option_revenue}"
        )

    if total_mart_discount_amount != total_source_discount_amount:
        raise ValueError(
            "Discount amount mismatch between order_item_options_clean and option_sales mart. "
            f"source_total={total_source_discount_amount}, "
            f"mart_total={total_mart_discount_amount}"
        )

    if total_mart_option_quantity != total_source_option_quantity:
        raise ValueError(
            "Option quantity mismatch between order_item_options_clean and option_sales mart. "
            f"source_total={total_source_option_quantity}, "
            f"mart_total={total_mart_option_quantity}"
        )

    (
        option_sales
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote option_sales mart to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()