import sys
import json
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType


REQUIRED_ARGS = [
    "JOB_NAME",
    "RAW_BASE_PATH",
    "SILVER_BASE_PATH",
    "INGEST_DATE",
]


LOCAL_CONFIG_PATH = Path("config/silver_date_dim_clean.local.json")


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
            "Copy config/silver_date_dim_clean.local.example.json to "
            "config/silver_date_dim_clean.local.json and fill in the values."
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

    print(f"Reading raw date_dim from: {raw_input_path}")
    print(f"Writing silver date_dim_clean to: {silver_output_path}")

    df_raw = spark.read.parquet(raw_input_path)
    raw_count = df_raw.count()

    print(f"Raw row count: {raw_count}")
    print(f"Raw columns: {df_raw.columns}")

    df = normalize_column_names(df_raw)

    # Defensive date parsing:
    # - If date_key arrives as a date/timestamp type, cast to date.
    # - If date_key arrives as a string like '01-01-2023', parse as dd-MM-yyyy.
    df_clean = (
        df
        .select(
            F.coalesce(
                F.to_date(F.col("date_key")),
                F.to_date(F.col("date_key").cast(StringType()), "dd-MM-yyyy"),
                F.to_date(F.col("date_key").cast(StringType()), "yyyy-MM-dd"),
            ).alias("date_key"),
            F.col("year").cast(IntegerType()).alias("year"),
            F.col("month").cast(IntegerType()).alias("month"),
            F.col("week").cast(IntegerType()).alias("week"),
            F.col("day_of_week").cast(StringType()).alias("day_of_week"),
            F.col("is_weekend").cast(BooleanType()).alias("is_weekend"),
            F.col("is_holiday").cast(BooleanType()).alias("is_holiday"),
            F.col("holiday_name").cast(StringType()).alias("holiday_name"),
        )
        .withColumn("day_of_month", F.dayofmonth(F.col("date_key")))
        .withColumn("quarter", F.quarter(F.col("date_key")))
        .withColumn("month_name", F.date_format(F.col("date_key"), "MMMM"))
        .withColumn(
            "is_valid_date_key",
            F.col("date_key").isNotNull(),
        )
        .withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )
    )

    clean_count = df_clean.count()

    invalid_date_key_count = df_clean.filter(~F.col("is_valid_date_key")).count()

    duplicate_date_key_count = (
        df_clean
        .groupBy("date_key")
        .count()
        .filter((F.col("date_key").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    print(f"Silver row count: {clean_count}")
    print(f"Invalid date_key rows: {invalid_date_key_count}")
    print(f"Duplicate date_key groups: {duplicate_date_key_count}")

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

    print(f"Successfully wrote silver date_dim_clean to: {silver_output_path}")

    job.commit()


if __name__ == "__main__":
    main()