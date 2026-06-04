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
    "FACT_ORDER_BASE_PATH",
    "DATE_DIM_SILVER_BASE_PATH",
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_daily_sales.local.json")


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
                "Copy config/mart_daily_sales.local.example.json to "
                "config/mart_daily_sales.local.json and fill in the values."
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

    fact_order_path = (
        f'{args["FACT_ORDER_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    date_dim_path = (
        f'{args["DATE_DIM_SILVER_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading gold fact_order from: {fact_order_path}")
    print(f"Reading silver date_dim_clean from: {date_dim_path}")
    print(f"Writing daily_sales mart to: {mart_output_path}")

    fact_order = spark.read.parquet(fact_order_path)
    date_dim = spark.read.parquet(date_dim_path)

    fact_order_count = fact_order.count()
    date_dim_count = date_dim.count()

    print(f"fact_order row count: {fact_order_count}")
    print(f"date_dim_clean row count: {date_dim_count}")
    print(f"fact_order columns: {fact_order.columns}")
    print(f"date_dim_clean columns: {date_dim.columns}")

    if fact_order_count <= 0:
        raise ValueError("fact_order source has zero rows.")

    if date_dim_count <= 0:
        raise ValueError("date_dim_clean source has zero rows.")

    daily_sales_base = (
        fact_order
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("customer_count"),
            F.countDistinct("restaurant_id").alias("restaurant_count"),
            F.sum("line_item_count").alias("line_item_count"),
            F.sum("distinct_line_item_count").alias("distinct_line_item_count"),
            F.sum("total_item_quantity").alias("total_item_quantity"),
            F.sum("gross_item_revenue").cast(DecimalType(14, 2)).alias("gross_item_revenue"),
            F.sum("option_revenue").cast(DecimalType(14, 2)).alias("option_revenue"),
            F.sum("discount_amount").cast(DecimalType(14, 2)).alias("discount_amount"),
            F.sum("net_order_revenue").cast(DecimalType(14, 2)).alias("net_revenue"),
            F.sum("option_row_count").alias("option_row_count"),
            F.sum(F.col("has_options").cast("int")).alias("orders_with_options"),
            F.sum(F.col("has_discount").cast("int")).alias("orders_with_discount"),
            F.sum(F.col("has_user_id").cast("int")).alias("orders_with_user_id"),
            F.sum((~F.col("all_line_keys_valid")).cast("int")).alias("orders_with_invalid_line_keys"),
        )
        .withColumn(
            "avg_order_value",
            (F.col("net_revenue") / F.col("order_count")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "avg_items_per_order",
            (F.col("total_item_quantity") / F.col("order_count")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "option_attach_rate",
            (F.col("orders_with_options") / F.col("order_count")).cast(DecimalType(14, 4)),
        )
        .withColumn(
            "customer_identification_rate",
            (F.col("orders_with_user_id") / F.col("order_count")).cast(DecimalType(14, 4)),
        )
    )

    daily_sales_base_count = daily_sales_base.count()

    print(f"daily_sales_base row count: {daily_sales_base_count}")
    
    fact_min_max_dates = (
        daily_sales_base
        .select(
            F.min("order_date").alias("fact_min_order_date"),
            F.max("order_date").alias("fact_max_order_date"),
        )
        .collect()[0]
    )
    
    date_dim_min_max_dates = (
        date_dim
        .select(
            F.min("date_key").alias("date_dim_min_date"),
            F.max("date_key").alias("date_dim_max_date"),
        )
        .collect()[0]
    )
    
    print(f"fact_order min order_date: {fact_min_max_dates['fact_min_order_date']}")
    print(f"fact_order max order_date: {fact_min_max_dates['fact_max_order_date']}")
    print(f"date_dim min date_key: {date_dim_min_max_dates['date_dim_min_date']}")
    print(f"date_dim max date_key: {date_dim_min_max_dates['date_dim_max_date']}")

    unmatched_date_count = (
        daily_sales_base.alias("sales")
        .join(
            date_dim.alias("dt"),
            F.col("sales.order_date") == F.col("dt.date_key"),
            how="left_anti",
        )
        .count()
    )
    
    print(f"Unmatched date_dim rows before fallback: {unmatched_date_count}")

    daily_sales = (
        daily_sales_base.alias("sales")
        .join(
            date_dim.alias("dt"),
            F.col("sales.order_date") == F.col("dt.date_key"),
            how="left",
        )
        .select(
            F.col("sales.order_date"),
    
            F.coalesce(F.col("dt.year"), F.year(F.col("sales.order_date"))).alias("year"),
            F.coalesce(F.col("dt.month"), F.month(F.col("sales.order_date"))).alias("month"),
            F.coalesce(F.col("dt.month_name"), F.date_format(F.col("sales.order_date"), "MMMM")).alias("month_name"),
            F.coalesce(F.col("dt.week"), F.weekofyear(F.col("sales.order_date"))).alias("week"),
            F.coalesce(F.col("dt.day_of_week"), F.date_format(F.col("sales.order_date"), "EEEE")).alias("day_of_week"),
            F.coalesce(F.col("dt.day_of_month"), F.dayofmonth(F.col("sales.order_date"))).alias("day_of_month"),
            F.coalesce(F.col("dt.quarter"), F.quarter(F.col("sales.order_date"))).alias("quarter"),
    
            F.coalesce(F.col("dt.is_weekend"), F.dayofweek(F.col("sales.order_date")).isin([1, 7])).alias("is_weekend"),
            F.coalesce(F.col("dt.is_holiday"), F.lit(False)).alias("is_holiday"),
            F.col("dt.holiday_name").alias("holiday_name"),
    
            F.col("sales.order_count"),
            F.col("sales.customer_count"),
            F.col("sales.restaurant_count"),
            F.col("sales.line_item_count"),
            F.col("sales.distinct_line_item_count"),
            F.col("sales.total_item_quantity"),
            F.col("sales.gross_item_revenue"),
            F.col("sales.option_revenue"),
            F.col("sales.discount_amount"),
            F.col("sales.net_revenue"),
            F.col("sales.avg_order_value"),
            F.col("sales.avg_items_per_order"),
            F.col("sales.option_row_count"),
            F.col("sales.orders_with_options"),
            F.col("sales.orders_with_discount"),
            F.col("sales.option_attach_rate"),
            F.col("sales.orders_with_user_id"),
            F.col("sales.customer_identification_rate"),
            F.col("sales.orders_with_invalid_line_keys"),
    
            (F.col("dt.date_key").isNull()).alias("used_derived_date_attributes"),
    
            F.lit(ingest_date).alias("ingest_date"),
        )
    )

    mart_count = daily_sales.count()

    unmatched_date_count = (
        daily_sales
        .filter(F.col("used_derived_date_attributes"))
        .count()
    )

    total_net_revenue = (
        daily_sales
        .select(F.sum("net_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_order_count = (
        daily_sales
        .select(F.sum("order_count").alias("total"))
        .collect()[0]["total"]
    )

    max_daily_net_revenue = (
        daily_sales
        .select(F.max("net_revenue").alias("max_daily_net_revenue"))
        .collect()[0]["max_daily_net_revenue"]
    )

    min_order_date = (
        daily_sales
        .select(F.min("order_date").alias("min_order_date"))
        .collect()[0]["min_order_date"]
    )

    max_order_date = (
        daily_sales
        .select(F.max("order_date").alias("max_order_date"))
        .collect()[0]["max_order_date"]
    )

    print(f"Daily sales mart row count: {mart_count}")
    print(f"Unmatched date_dim rows: {unmatched_date_count}")
    print(f"Total order count in daily_sales mart: {total_order_count}")
    print(f"Total net revenue in daily_sales mart: {total_net_revenue}")
    print(f"Max daily net revenue: {max_daily_net_revenue}")
    print(f"Min order_date: {min_order_date}")
    print(f"Max order_date: {max_order_date}")

    if mart_count <= 0:
        raise ValueError("daily_sales mart produced zero rows.")

    if unmatched_date_count > 0:
        print(
            "WARNING: Some daily_sales rows did not match date_dim_clean. "
            "Derived date attributes from order_date for unmatched rows. "
            f"unmatched_date_count={unmatched_date_count}"
        )

    source_total_net_revenue = (
        fact_order
        .select(F.sum("net_order_revenue").alias("total"))
        .collect()[0]["total"]
    )

    if total_net_revenue != source_total_net_revenue:
        raise ValueError(
            "Revenue mismatch between fact_order and daily_sales mart. "
            f"fact_order_total={source_total_net_revenue}, "
            f"daily_sales_total={total_net_revenue}"
        )

    daily_sales_clean_to_write = daily_sales.drop("ingest_date")

    (
        daily_sales_clean_to_write
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote daily_sales mart to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()