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
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_restaurant_item_sales.local.json")


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
                "Copy config/mart_restaurant_item_sales.local.example.json to "
                "config/mart_restaurant_item_sales.local.json and fill in the values."
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

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading gold fact_order_line from: {fact_order_line_path}")
    print(f"Writing restaurant_item_sales mart to: {mart_output_path}")

    fact_order_line = spark.read.parquet(fact_order_line_path)
    source_count = fact_order_line.count()

    print(f"fact_order_line row count: {source_count}")
    print(f"fact_order_line columns: {fact_order_line.columns}")

    if source_count <= 0:
        raise ValueError("fact_order_line source has zero rows.")

    restaurant_item_sales = (
        fact_order_line
        .groupBy("restaurant_id", "item_category", "item_name")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("customer_count"),
            F.count(F.lit(1)).alias("line_item_count"),
            F.sum("item_quantity").alias("total_item_quantity"),

            F.avg("item_price").cast(DecimalType(14, 2)).alias("avg_item_price"),
            F.min("item_price").cast(DecimalType(14, 2)).alias("min_item_price"),
            F.max("item_price").cast(DecimalType(14, 2)).alias("max_item_price"),

            F.sum("gross_item_revenue").cast(DecimalType(14, 2)).alias("gross_item_revenue"),
            F.sum("option_revenue").cast(DecimalType(14, 2)).alias("option_revenue"),
            F.sum("discount_amount").cast(DecimalType(14, 2)).alias("discount_amount"),
            F.sum("net_line_revenue").cast(DecimalType(14, 2)).alias("net_revenue"),

            F.sum("option_row_count").alias("option_row_count"),
            F.sum(F.col("has_options").cast("int")).alias("line_items_with_options"),
            F.sum(F.col("has_discount").cast("int")).alias("line_items_with_discount"),
        )
        .withColumn(
            "avg_net_revenue_per_line",
            (F.col("net_revenue") / F.col("line_item_count")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "avg_net_revenue_per_unit",
            (F.col("net_revenue") / F.col("total_item_quantity")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "option_attach_rate",
            (F.col("line_items_with_options") / F.col("line_item_count")).cast(DecimalType(14, 4)),
        )
        .withColumn(
            "discount_attach_rate",
            (F.col("line_items_with_discount") / F.col("line_item_count")).cast(DecimalType(14, 4)),
        )
        .withColumn("ingest_date", F.lit(ingest_date))
    )

    mart_count = restaurant_item_sales.count()

    total_source_net_revenue = (
        fact_order_line
        .select(F.sum("net_line_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_mart_net_revenue = (
        restaurant_item_sales
        .select(F.sum("net_revenue").alias("total"))
        .collect()[0]["total"]
    )

    total_source_line_item_count = source_count

    total_mart_line_item_count = (
        restaurant_item_sales
        .select(F.sum("line_item_count").alias("total"))
        .collect()[0]["total"]
    )

    total_source_item_quantity = (
        fact_order_line
        .select(F.sum("item_quantity").alias("total"))
        .collect()[0]["total"]
    )

    total_mart_item_quantity = (
        restaurant_item_sales
        .select(F.sum("total_item_quantity").alias("total"))
        .collect()[0]["total"]
    )

    distinct_restaurant_count = (
        restaurant_item_sales
        .select("restaurant_id")
        .distinct()
        .count()
    )

    distinct_restaurant_item_count = (
        restaurant_item_sales
        .select("restaurant_id", "item_category", "item_name")
        .distinct()
        .count()
    )

    top_restaurant_item = (
        restaurant_item_sales
        .orderBy(F.col("net_revenue").desc())
        .select("restaurant_id", "item_category", "item_name", "net_revenue")
        .limit(1)
        .collect()
    )

    print(f"Restaurant item sales mart row count: {mart_count}")
    print(f"Distinct restaurants in restaurant_item_sales mart: {distinct_restaurant_count}")
    print(f"Distinct restaurant/item/category combinations: {distinct_restaurant_item_count}")
    print(f"Total source net revenue: {total_source_net_revenue}")
    print(f"Total restaurant_item_sales net revenue: {total_mart_net_revenue}")
    print(f"Total source line item count: {total_source_line_item_count}")
    print(f"Total restaurant_item_sales line item count: {total_mart_line_item_count}")
    print(f"Total source item quantity: {total_source_item_quantity}")
    print(f"Total restaurant_item_sales item quantity: {total_mart_item_quantity}")

    if top_restaurant_item:
        row = top_restaurant_item[0]
        print(
            "Top restaurant item by net revenue: "
            f"restaurant_id={row['restaurant_id']}, "
            f"category={row['item_category']}, "
            f"item_name={row['item_name']}, "
            f"net_revenue={row['net_revenue']}"
        )

    if mart_count <= 0:
        raise ValueError("restaurant_item_sales mart produced zero rows.")

    if mart_count != distinct_restaurant_item_count:
        raise ValueError(
            "restaurant_item_sales should have one row per "
            "restaurant_id + item_category + item_name. "
            f"mart_count={mart_count}, "
            f"distinct_restaurant_item_count={distinct_restaurant_item_count}"
        )

    if total_mart_net_revenue != total_source_net_revenue:
        raise ValueError(
            "Revenue mismatch between fact_order_line and restaurant_item_sales mart. "
            f"fact_order_line_total={total_source_net_revenue}, "
            f"restaurant_item_sales_total={total_mart_net_revenue}"
        )

    if total_mart_line_item_count != total_source_line_item_count:
        raise ValueError(
            "Line item count mismatch between fact_order_line and restaurant_item_sales mart. "
            f"fact_order_line_count={total_source_line_item_count}, "
            f"restaurant_item_sales_line_item_count={total_mart_line_item_count}"
        )

    if total_mart_item_quantity != total_source_item_quantity:
        raise ValueError(
            "Item quantity mismatch between fact_order_line and restaurant_item_sales mart. "
            f"fact_order_line_quantity={total_source_item_quantity}, "
            f"restaurant_item_sales_quantity={total_mart_item_quantity}"
        )

    restaurant_item_sales_clean_to_write = restaurant_item_sales.drop("ingest_date")

    (
        restaurant_item_sales_clean_to_write
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote restaurant_item_sales mart to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()