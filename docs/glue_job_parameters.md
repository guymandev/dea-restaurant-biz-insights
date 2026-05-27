# Glue Job Parameters

## restaurant_ingest_order_items_to_raw

| Parameter | Description | Example |
|---|---|---|
| `--AWS_REGION` | AWS region where Glue/RDS/Secrets Manager are deployed | `us-east-2` |
| `--JDBC_URL` | JDBC URL for SQL Server database | `jdbc:sqlserver://<rds-endpoint>:1433;databaseName=<db-name>;encrypt=true;trustServerCertificate=true;` |
| `--DB_TABLE` | SQL Server source table | `dbo.order_items` |
| `--DB_USER` | SQL Server login username | `<sql-server-user>` |
| `--SECRET_ID` | Secrets Manager secret name or ARN containing DB password | `<secret-name-or-arn>` |
| `--S3_OUTPUT_PATH` | Target S3 raw prefix | `s3://<bucket-name>/raw/order_items` |