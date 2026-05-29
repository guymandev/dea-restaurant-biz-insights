import sys
import json
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, IntegerType, StringType


REQUIRED_ARGS = [
    "JOB_NAME",
    "RAW_BASE_PATH",
    "SILVER_BASE_PATH",
    "INGEST_DATE",
]


LOCAL_CONFIG_PATH = Path("config/silver_order_items_clean.local.json")


def running_in_glue() -> bool:
    return "--JOB_NAME" in sys.argv


def load_args() -> dict:
    """
    Load arguments from AWS Glue job parameters when running in Glue.
    Load arguments from local JSON config when running locally.

    Note: local execution still requires a compatible Spark/Glue runtime.
    The local config is primarily useful for documentation and parameter tracking.
    """
    if running_in_glue():
        return getResolvedOptions(sys.argv, REQUIRED_ARGS)

    if not LOCAL_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Local config file not found: {LOCAL_CONFIG_PATH}. "
            "Copy config/silver_order_items_clean.local.example.json to "
            "config/silver_order_items_clean.local.json and fill in the values."
        )

    with LOCAL_CONFIG_PATH.open("r", encoding="utf-8") as f:
        args = json.load(f)

    missing = [key for key in REQUIRED_ARGS if key not in args or not args[key]]

    if missing:
        raise ValueError(f"Missing required local config values: {missing}")

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

    print(f"Reading raw order_items from: {raw_input_path}")
    print(f"Writing silver order_items_clean to: {silver_output_path}")

    df_raw = spark.read.parquet(raw_input_path)
    raw_count = df_raw.count()

    print(f"Raw row count: {raw_count}")
    print(f"Raw columns: {df_raw.columns}")

    df = normalize_column_names(df_raw)

    df_clean = (
        df
        .select(
            F.col("app_name").cast(StringType()).alias("app_name"),
            F.col("restaurant_id").cast(StringType()).alias("restaurant_id"),
            F.to_timestamp("creation_time_utc").alias("creation_time_utc"),
            F.col("order_id").cast(StringType()).alias("order_id"),
            F.col("user_id").cast(StringType()).alias("user_id"),
            F.col("printed_card_number").cast(StringType()).alias("printed_card_number"),
            F.col("is_loyalty").cast(BooleanType()).alias("is_loyalty"),
            F.col("currency").cast(StringType()).alias("currency"),
            F.col("lineitem_id").cast(StringType()).alias("lineitem_id"),
            F.col("item_category").cast(StringType()).alias("item_category"),
            F.col("item_name").cast(StringType()).alias("item_name"),
            F.col("item_price").cast(DecimalType(12, 2)).alias("item_price"),
            F.col("item_quantity").cast(IntegerType()).alias("item_quantity"),
        )
        .withColumn("order_date", F.to_date(F.col("creation_time_utc")))
        .withColumn(
            "gross_item_revenue",
            (F.col("item_price") * F.col("item_quantity")).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "is_valid_order_item_key",
            F.col("order_id").isNotNull() & F.col("lineitem_id").isNotNull(),
        )
        .withColumn(
            "has_user_id",
            F.col("user_id").isNotNull(),
        )
        .withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )
    )

    clean_count = df_clean.count()

    invalid_key_count = df_clean.filter(~F.col("is_valid_order_item_key")).count()
    missing_user_id_count = df_clean.filter(~F.col("has_user_id")).count()
    invalid_timestamp_count = df_clean.filter(F.col("creation_time_utc").isNull()).count()

    print(f"Silver row count: {clean_count}")
    print(f"Invalid order item key rows: {invalid_key_count}")
    print(f"Missing user_id rows: {missing_user_id_count}")
    print(f"Invalid creation_time_utc rows: {invalid_timestamp_count}")

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

    print(f"Successfully wrote silver order_items_clean to: {silver_output_path}")

    job.commit()


if __name__ == "__main__":
    main()