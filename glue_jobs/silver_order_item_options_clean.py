import sys
import json
from pathlib import Path
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType


REQUIRED_ARGS = [
    "JOB_NAME",
    "RAW_BASE_PATH",
    "SILVER_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/silver_order_item_options_clean.local.json")


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
                "Copy config/silver_order_item_options_clean.local.example.json to "
                "config/silver_order_item_options_clean.local.json and fill in the values."
            )

        with LOCAL_CONFIG_PATH.open("r", encoding="utf-8") as f:
            args = json.load(f)

    missing = [key for key in REQUIRED_ARGS if key not in args or not args[key]]

    if missing:
        raise ValueError(f"Missing required config values: {missing}")

    args["INGEST_DATE"] = args.get("INGEST_DATE") or default_ingest_date()

    return args


def normalize_column_names(df):
    """
    Normalize column names to lowercase snake_case.
    """
    for col_name in df.columns:
        normalized = col_name.strip().lower().replace(" ", "_")
        if normalized != col_name:
            df = df.withColumnRenamed(col_name, normalized)

    return df


def main() -> None:
    args = load_args()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    ingest_date = args["INGEST_DATE"]

    raw_input_path = (
        f'{args["RAW_BASE_PATH"].rstrip("/")}/ingest_date={ingest_date}/'
    )

    silver_output_path = (
        f'{args["SILVER_BASE_PATH"].rstrip("/")}/ingest_date={ingest_date}/'
    )

    print(f"Reading raw order_item_options from: {raw_input_path}")
    print(f"Writing silver order_item_options_clean to: {silver_output_path}")

    df_raw = spark.read.parquet(raw_input_path)
    raw_count = df_raw.count()

    print(f"Raw row count: {raw_count}")
    print(f"Raw columns: {df_raw.columns}")

    df = normalize_column_names(df_raw)

    df_clean = (
        df
        .select(
            F.col("order_id").cast(StringType()).alias("order_id"),
            F.col("lineitem_id").cast(StringType()).alias("lineitem_id"),
            F.col("option_group_name").cast(StringType()).alias("option_group_name"),
            F.col("option_name").cast(StringType()).alias("option_name"),
            F.col("option_price").cast(DecimalType(12, 2)).alias("option_price"),
            F.col("option_quantity").cast(IntegerType()).alias("option_quantity"),
        )
        .withColumn(
            "option_revenue",
            (F.col("option_price") * F.col("option_quantity")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "is_discount",
            F.col("option_price") < F.lit(0),
        )
        .withColumn(
            "discount_amount",
            F.when(
                F.col("option_price") < F.lit(0),
                F.abs(F.col("option_revenue"))
            ).otherwise(F.lit(0).cast(DecimalType(14, 2))),
        )
        .withColumn(
            "is_valid_option_key",
            F.col("order_id").isNotNull() & F.col("lineitem_id").isNotNull(),
        )
        .withColumn(
            "candidate_option_key",
            F.concat_ws(
                "|",
                F.col("order_id"),
                F.col("lineitem_id"),
                F.col("option_group_name"),
                F.col("option_name"),
            ),
        )
        .withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )
    )

    clean_count = df_clean.count()

    invalid_key_count = df_clean.filter(~F.col("is_valid_option_key")).count()
    discount_row_count = df_clean.filter(F.col("is_discount")).count()
    null_option_price_count = df_clean.filter(F.col("option_price").isNull()).count()
    null_option_quantity_count = df_clean.filter(F.col("option_quantity").isNull()).count()

    duplicate_candidate_key_groups = (
        df_clean
        .groupBy(
            "order_id",
            "lineitem_id",
            "option_group_name",
            "option_name",
        )
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_candidate_key_group_count = duplicate_candidate_key_groups.count()

    duplicate_candidate_key_row_count = (
        duplicate_candidate_key_groups
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows"))
        .collect()[0]["duplicate_rows"]
    )

    duplicate_candidate_key_row_count = duplicate_candidate_key_row_count or 0

    print(f"Silver row count: {clean_count}")
    print(f"Invalid option key rows: {invalid_key_count}")
    print(f"Discount rows: {discount_row_count}")
    print(f"Null option_price rows: {null_option_price_count}")
    print(f"Null option_quantity rows: {null_option_quantity_count}")
    print(f"Duplicate candidate option key groups: {duplicate_candidate_key_group_count}")
    print(f"Duplicate candidate option key rows: {duplicate_candidate_key_row_count}")

    if clean_count != raw_count:
        raise ValueError(
            f"Row count mismatch: raw_count={raw_count}, clean_count={clean_count}"
        )

    (
        df_clean
        .write
        .mode("overwrite")
        .parquet(silver_output_path)
    )

    print(
        "Successfully wrote silver order_item_options_clean "
        f"to: {silver_output_path}"
    )

    job.commit()


if __name__ == "__main__":
    main()