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
from pyspark.sql.types import DecimalType


REQUIRED_ARGS = [
    "JOB_NAME",
    "FACT_ORDER_BASE_PATH",
    "MART_BASE_PATH",
]

OPTIONAL_ARGS = [
    "INGEST_DATE",
]

LOCAL_CONFIG_PATH = Path("config/mart_customer_daily_clv.local.json")


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
                "Copy config/mart_customer_daily_clv.local.example.json to "
                "config/mart_customer_daily_clv.local.json and fill in the values."
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

    mart_output_path = (
        f'{args["MART_BASE_PATH"].rstrip("/")}/'
        f"ingest_date={ingest_date}/"
    )

    print(f"Reading gold fact_order from: {fact_order_path}")
    print(f"Writing mart customer_daily_clv to: {mart_output_path}")

    fact_order = spark.read.parquet(fact_order_path)
    fact_order_count = fact_order.count()

    print(f"fact_order row count: {fact_order_count}")
    print(f"fact_order columns: {fact_order.columns}")

    customer_orders = (
        fact_order
        .filter(F.col("user_id").isNotNull())
        .select(
            "user_id",
            "order_id",
            "order_date",
            "restaurant_id",
            "is_loyalty",
            "gross_item_revenue",
            "option_revenue",
            "discount_amount",
            "net_order_revenue",
        )
    )

    customer_order_count = customer_orders.count()
    print(f"Customer-attributed order count: {customer_order_count}")
    
    customer_order_total_net_revenue = (
        customer_orders
        .select(F.sum("net_order_revenue").alias("total"))
        .collect()[0]["total"]
    )
    
    print(f"Customer-attributed order total net revenue: {customer_order_total_net_revenue}")

    daily_customer_spend = (
        customer_orders
        .groupBy("user_id", "order_date")
        .agg(
            F.countDistinct("order_id").alias("daily_order_count"),
            F.countDistinct("restaurant_id").alias("daily_restaurant_count"),
            F.max(F.col("is_loyalty").cast("int")).alias("is_loyalty_customer_day_int"),
            F.sum("gross_item_revenue").cast(DecimalType(14, 2)).alias("daily_gross_item_revenue"),
            F.sum("option_revenue").cast(DecimalType(14, 2)).alias("daily_option_revenue"),
            F.sum("discount_amount").cast(DecimalType(14, 2)).alias("daily_discount_amount"),
            F.sum("net_order_revenue").cast(DecimalType(14, 2)).alias("daily_net_revenue"),
        )
        .withColumn(
            "is_loyalty_customer_day",
            F.col("is_loyalty_customer_day_int") == F.lit(1),
        )
        .drop("is_loyalty_customer_day_int")
    )

    daily_spend_count = daily_customer_spend.count()
    
    daily_spend_total_net_revenue = (
        daily_customer_spend
        .select(F.sum("daily_net_revenue").alias("total"))
        .collect()[0]["total"]
    )
    
    print(f"Daily customer spend row count: {daily_spend_count}")
    print(f"Daily customer spend total net revenue: {daily_spend_total_net_revenue}")

    customer_window = (
        Window
        .partitionBy("user_id")
        .orderBy("order_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    customer_lifetime_window = Window.partitionBy("user_id")

    mart = (
        daily_customer_spend
        .withColumn(
            "cumulative_order_count",
            F.sum("daily_order_count").over(customer_window),
        )
        .withColumn(
            "cumulative_net_revenue",
            F.sum("daily_net_revenue").over(customer_window).cast(DecimalType(14, 2)),
        )
        .withColumn(
            "first_order_date",
            F.min("order_date").over(customer_lifetime_window),
        )
        .withColumn(
            "latest_order_date",
            F.max("order_date").over(customer_lifetime_window),
        )
        .withColumn(
            "customer_lifetime_days",
            F.datediff(F.col("latest_order_date"), F.col("first_order_date")) + F.lit(1),
        )
    )

    # Assign simple CLV tiers based on cumulative net revenue percentiles.
    # ntile(4): 1 = lowest quartile, 4 = highest quartile.
    clv_tier_window = Window.orderBy(F.col("cumulative_net_revenue").asc())

    mart = (
        mart
        .withColumn("clv_quartile", F.ntile(4).over(clv_tier_window))
        .withColumn(
            "clv_tier",
            F.when(F.col("clv_quartile") == 4, F.lit("high"))
            .when(F.col("clv_quartile") == 3, F.lit("medium_high"))
            .when(F.col("clv_quartile") == 2, F.lit("medium_low"))
            .otherwise(F.lit("low"))
        )
        .withColumn("ingest_date", F.lit(ingest_date))
    )

    mart_count = mart.count()

    mart_total_daily_net_revenue = (
        mart
        .select(F.sum("daily_net_revenue").alias("total"))
        .collect()[0]["total"]
    )

    distinct_customer_count = mart.select("user_id").distinct().count()

    max_cumulative_net_revenue = (
        mart
        .select(F.max("cumulative_net_revenue").alias("max_clv"))
        .collect()[0]["max_clv"]
    )
    
    print(f"Mart customer_daily_clv row count: {mart_count}")
    print(f"Distinct customers in mart: {distinct_customer_count}")
    print(f"Final mart total daily net revenue: {mart_total_daily_net_revenue}")
    print(f"Max cumulative net revenue: {max_cumulative_net_revenue}")
   

    if mart_count <= 0:
        raise ValueError("customer_daily_clv mart produced zero rows.")
        
    if mart_total_daily_net_revenue != daily_spend_total_net_revenue:
        raise ValueError(
            "Revenue total changed between daily_customer_spend and final mart. "
            f"daily_spend_total={daily_spend_total_net_revenue}, "
            f"mart_total={mart_total_daily_net_revenue}"
        )

    (
        mart
        .write
        .mode("overwrite")
        .parquet(mart_output_path)
    )

    print(f"Successfully wrote mart customer_daily_clv to: {mart_output_path}")

    job.commit()


if __name__ == "__main__":
    main()