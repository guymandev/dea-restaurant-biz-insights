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
    "CUSTOMER_CLV_SNAPSHOT_BASE_PATH",
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_customer_rfm.local.json")


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
                "Copy config/mart_customer_rfm.local.example.json to "
                "config/mart_customer_rfm.local.json and fill in the values."
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

    customer_clv_snapshot_path = (
        f'{args["CUSTOMER_CLV_SNAPSHOT_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading customer_clv_snapshot mart from: {customer_clv_snapshot_path}")
    print(f"Writing customer_rfm mart to: {mart_output_path}")

    customer_snapshot = spark.read.parquet(customer_clv_snapshot_path)
    snapshot_count = customer_snapshot.count()

    print(f"customer_clv_snapshot row count: {snapshot_count}")
    print(f"customer_clv_snapshot columns: {customer_snapshot.columns}")

    if snapshot_count <= 0:
        raise ValueError("customer_clv_snapshot source has zero rows.")

    # Use the latest order date in the dataset as the as-of date.
    # This avoids comparing historical/sample data to the real current date.
    as_of_date = (
        customer_snapshot
        .select(F.max("latest_order_date").alias("as_of_date"))
        .collect()[0]["as_of_date"]
    )

    print(f"RFM as_of_date: {as_of_date}")

    rfm_base = (
        customer_snapshot
        .select(
            "user_id",
            "first_order_date",
            "latest_order_date",
            "customer_lifetime_days",
            "lifetime_order_count",
            "lifetime_net_revenue",
            "latest_clv_quartile" if "latest_clv_quartile" in customer_snapshot.columns else "clv_quartile",
            "latest_clv_tier" if "latest_clv_tier" in customer_snapshot.columns else "clv_tier",
        )
    )

    # Normalize CLV column names in case this job is run against either
    # the pre-refactor or post-refactor customer_clv_snapshot schema.
    if "latest_clv_quartile" in rfm_base.columns:
        rfm_base = rfm_base.withColumnRenamed("latest_clv_quartile", "clv_quartile")

    if "latest_clv_tier" in rfm_base.columns:
        rfm_base = rfm_base.withColumnRenamed("latest_clv_tier", "clv_tier")

    rfm_base = (
        rfm_base
        .withColumn(
            "recency_days",
            F.datediff(F.lit(as_of_date), F.col("latest_order_date")),
        )
    )

    # RFM scoring:
    # Recency: lower recency_days is better, so order ascending.
    # Frequency: higher lifetime_order_count is better, so order ascending then map 4 as best.
    # Monetary: higher lifetime_net_revenue is better, so order ascending then map 4 as best.
    #
    # ntile(4) creates quartile scores from 1 to 4.
    # For recency, ntile ascending already makes most recent customers score 1,
    # so we invert it to make 4 best.
    recency_window = Window.orderBy(F.col("recency_days").asc())
    frequency_window = Window.orderBy(F.col("lifetime_order_count").asc())
    monetary_window = Window.orderBy(F.col("lifetime_net_revenue").asc())

    rfm_scored = (
        rfm_base
        .withColumn("recency_quartile_raw", F.ntile(4).over(recency_window))
        .withColumn("frequency_score", F.ntile(4).over(frequency_window))
        .withColumn("monetary_score", F.ntile(4).over(monetary_window))
        .withColumn(
            "recency_score",
            F.lit(5) - F.col("recency_quartile_raw"),
        )
        .drop("recency_quartile_raw")
        .withColumn(
            "rfm_score",
            (
                F.col("recency_score").cast("string")
                + F.col("frequency_score").cast("string")
                + F.col("monetary_score").cast("string")
            ),
        )
        .withColumn(
            "rfm_total_score",
            F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"),
        )
    )

    rfm_segmented = (
        rfm_scored
        .withColumn(
            "rfm_segment",
            F.when(
                (F.col("recency_score") >= 4)
                & (F.col("frequency_score") >= 4)
                & (F.col("monetary_score") >= 4),
                F.lit("champions"),
            )
            .when(
                (F.col("recency_score") >= 3)
                & (F.col("frequency_score") >= 3)
                & (F.col("monetary_score") >= 3),
                F.lit("loyal_high_value"),
            )
            .when(
                (F.col("recency_score") >= 4)
                & (F.col("frequency_score") <= 2),
                F.lit("new_or_promising"),
            )
            .when(
                (F.col("recency_score") <= 2)
                & (F.col("frequency_score") >= 3)
                & (F.col("monetary_score") >= 3),
                F.lit("at_risk_high_value"),
            )
            .when(
                (F.col("recency_score") <= 2)
                & (F.col("frequency_score") <= 2),
                F.lit("hibernating"),
            )
            .otherwise(F.lit("needs_attention"))
        )
        .withColumn("rfm_as_of_date", F.lit(as_of_date))
        .withColumn("ingest_date", F.lit(ingest_date))
    )

    mart_count = rfm_segmented.count()
    distinct_customer_count = rfm_segmented.select("user_id").distinct().count()

    segment_counts = (
        rfm_segmented
        .groupBy("rfm_segment")
        .count()
        .collect()
    )

    total_lifetime_net_revenue = (
        rfm_segmented
        .select(F.sum("lifetime_net_revenue").alias("total"))
        .collect()[0]["total"]
    )

    print(f"Customer RFM mart row count: {mart_count}")
    print(f"Distinct customers in RFM mart: {distinct_customer_count}")
    print(f"Total lifetime net revenue in RFM mart: {total_lifetime_net_revenue}")

    segment_count_map = {
        row["rfm_segment"]: row["count"]
        for row in segment_counts
    }

    for segment in [
        "champions",
        "loyal_high_value",
        "new_or_promising",
        "at_risk_high_value",
        "needs_attention",
        "hibernating",
    ]:
        print(f"RFM segment count - {segment}: {segment_count_map.get(segment, 0)}")

    if mart_count <= 0:
        raise ValueError("customer_rfm mart produced zero rows.")

    if mart_count != distinct_customer_count:
        raise ValueError(
            "customer_rfm should have one row per user_id. "
            f"mart_count={mart_count}, "
            f"distinct_customer_count={distinct_customer_count}"
        )

    rfm_segmented_clean_to_write = rfm_segmented.drop("ingest_date")

    (
        rfm_segmented_clean_to_write
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote customer_rfm mart to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()