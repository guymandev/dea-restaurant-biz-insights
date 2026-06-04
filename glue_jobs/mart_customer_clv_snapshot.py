import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


REQUIRED_ARGS = [
    "JOB_NAME",
    "CUSTOMER_DAILY_CLV_BASE_PATH",
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_customer_clv_snapshot.local.json")


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
                "Copy config/mart_customer_clv_snapshot.local.example.json to "
                "config/mart_customer_clv_snapshot.local.json and fill in the values."
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

    customer_daily_clv_path = (
        f'{args["CUSTOMER_DAILY_CLV_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading customer_daily_clv mart from: {customer_daily_clv_path}")
    print(f"Writing customer_clv_snapshot mart to: {mart_output_path}")

    daily_clv = spark.read.parquet(customer_daily_clv_path)
    daily_clv_count = daily_clv.count()

    print(f"customer_daily_clv row count: {daily_clv_count}")
    print(f"customer_daily_clv columns: {daily_clv.columns}")

    latest_customer_window = (
        Window
        .partitionBy("user_id")
        .orderBy(
            F.col("order_date").desc(),
            F.col("cumulative_net_revenue").desc(),
        )
    )

    snapshot_base = (
        daily_clv
        .withColumn("customer_recency_rank", F.row_number().over(latest_customer_window))
        .filter(F.col("customer_recency_rank") == F.lit(1))
        .select(
            "user_id",
            F.col("first_order_date"),
            F.col("latest_order_date"),
            F.col("customer_lifetime_days"),
            F.col("cumulative_order_count").alias("lifetime_order_count"),
            F.col("cumulative_net_revenue").alias("lifetime_net_revenue"),
            F.col("daily_order_count").alias("latest_daily_order_count"),
            F.col("daily_net_revenue").alias("latest_daily_net_revenue"),
            F.col("daily_restaurant_count").alias("latest_daily_restaurant_count"),
            F.col("is_loyalty_customer_day").alias("latest_is_loyalty_customer_day"),
        )
    )

    # Assign CLV tiers at the customer snapshot grain.
    # This ranks customers against other customers based on lifetime_net_revenue.
    customer_clv_window = Window.orderBy(F.col("lifetime_net_revenue").asc())

    snapshot = (
        snapshot_base
        .withColumn(
            "clv_quartile",
            F.ntile(4).over(customer_clv_window),
        )
        .withColumn(
            "clv_tier",
            F.when(F.col("clv_quartile") == 4, F.lit("high"))
            .when(F.col("clv_quartile") == 3, F.lit("medium_high"))
            .when(F.col("clv_quartile") == 2, F.lit("medium_low"))
            .otherwise(F.lit("low"))
        )
        .withColumn("ingest_date", F.lit(ingest_date))
    )

    snapshot_count = snapshot.count()
    distinct_customer_count = snapshot.select("user_id").distinct().count()

    total_lifetime_net_revenue = (
        snapshot
        .select(F.sum("lifetime_net_revenue").alias("total"))
        .collect()[0]["total"]
    )

    max_lifetime_net_revenue = (
        snapshot
        .select(F.max("lifetime_net_revenue").alias("max_lifetime_net_revenue"))
        .collect()[0]["max_lifetime_net_revenue"]
    )

    tier_counts = (
        snapshot
        .groupBy("clv_tier")
        .count()
        .collect()
    )

    tier_count_map = {
        row["clv_tier"]: row["count"]
        for row in tier_counts
    }

    print(f"Customer CLV snapshot row count: {snapshot_count}")
    print(f"Distinct customers in snapshot: {distinct_customer_count}")
    print(f"Total lifetime net revenue across customers: {total_lifetime_net_revenue}")
    print(f"Max lifetime net revenue: {max_lifetime_net_revenue}")

    for tier in ["high", "medium_high", "medium_low", "low"]:
        print(f"CLV tier count - {tier}: {tier_count_map.get(tier, 0)}")

    if snapshot_count <= 0:
        raise ValueError("customer_clv_snapshot mart produced zero rows.")

    if snapshot_count != distinct_customer_count:
        raise ValueError(
            "customer_clv_snapshot should have one row per user_id. "
            f"snapshot_count={snapshot_count}, "
            f"distinct_customer_count={distinct_customer_count}"
        )

    snapshot_clean_to_write = snapshot.drop("ingest_date")

    (
        snapshot_clean_to_write
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote customer_clv_snapshot mart to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()