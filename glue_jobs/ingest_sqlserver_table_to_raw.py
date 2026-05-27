import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


REQUIRED_ARGS = [
    "JOB_NAME",
    "AWS_REGION",
    "JDBC_URL",
    "DB_TABLE",
    "DB_USER",
    "SECRET_ID",
    "S3_OUTPUT_PATH",
]


def get_secret_password(secret_id: str, region_name: str) -> str:
    """
    Supports either:
    1. plaintext secret containing only the password
    2. JSON secret containing a password-like key
    """
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]

    try:
        secret_json = json.loads(secret_string)

        for key in ["password", "Password", "db_password", "DB_PASSWORD"]:
            if key in secret_json:
                return secret_json[key]

        raise KeyError(
            f"Secret {secret_id} is JSON, but no password-like key was found."
        )

    except json.JSONDecodeError:
        return secret_string


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

    config_path = Path("config/ingest_order_items.local.json")

    if not config_path.exists():
        raise FileNotFoundError(
            f"Local config file not found: {config_path}. "
            "Copy config/ingest_order_items.local.example.json to "
            "config/ingest_order_items.local.json and fill in the values."
        )

    with config_path.open("r", encoding="utf-8") as f:
        args = json.load(f)

    missing = [key for key in REQUIRED_ARGS if key not in args or not args[key]]

    if missing:
        raise ValueError(f"Missing required local config values: {missing}")

    return args


def main() -> None:
    args = load_args()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    db_password = get_secret_password(
        secret_id=args["SECRET_ID"],
        region_name=args["AWS_REGION"],
    )

    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f'{args["S3_OUTPUT_PATH"].rstrip("/")}/ingest_date={ingest_date}/'

    print(f"Starting ingestion for table: {args['DB_TABLE']}")
    print(f"Writing output to: {output_path}")

    df = (
        spark.read.format("jdbc")
        .option("url", args["JDBC_URL"])
        .option("dbtable", args["DB_TABLE"])
        .option("user", args["DB_USER"])
        .option("password", db_password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    row_count = df.count()
    print(f"Rows read from {args['DB_TABLE']}: {row_count}")

    df.write.mode("overwrite").parquet(output_path)

    print(f"Successfully wrote {row_count} rows to {output_path}")

    job.commit()


if __name__ == "__main__":
    main()