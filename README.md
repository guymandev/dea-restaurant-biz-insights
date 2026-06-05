# Restaurant Business Insights Pipeline

## Project Overview

This project implements an end-to-end data engineering and analytics pipeline for restaurant transaction data. The project is implemented almost entirely in AWS, with the exception of the final presentation layer, which uses Streamlit. The pipeline ingests source data from an RDS SQL Server database, processes it through AWS Glue PySpark jobs, stores curated datasets in Amazon S3, catalogs the resulting mart tables with AWS Glue Crawlers, validates the outputs with Amazon Athena, and presents business insights in a deployed Streamlit dashboard.

The project follows a layered lakehouse-style architecture:

```text
SQL Server → AWS Glue → S3 Raw → S3 Silver → S3 Gold → S3 Marts → Glue Data Catalog → Athena → Streamlit
```

The final output is a set of curated analytical marts that support executive, restaurant, menu item, customer, and option/modifier reporting.

## Business Goals

The project was designed to answer common restaurant business questions such as:

* What is total revenue and order volume across the business?
* Which restaurants generate the most revenue?
* Which menu items and categories perform best?
* Which item and restaurant/item combinations are top performers?
* How do customers segment by lifetime value and RFM behavior?
* Which modifiers/options generate the most revenue or quantity?
* Are the mart outputs internally consistent and reconcilable to the pipeline logs?

## Technology Stack

| Area                  | Technology                                      |
| --------------------- | ----------------------------------------------- |
| Source Database       | Amazon RDS SQL Server                           |
| Data Processing       | AWS Glue PySpark                                |
| Orchestration         | AWS Glue Workflow and Triggers                  |
| Storage               | Amazon S3                                       |
| Data Lake Layers      | Raw, Silver, Gold, Marts                        |
| Metadata Catalog      | AWS Glue Data Catalog                           |
| Query Engine          | Amazon Athena                                   |
| Dashboard             | Streamlit                                       |
| Dashboard Query Layer | `awswrangler`, `boto3`, Athena                  |
| Visualization         | Plotly                                          |
| Validation            | Glue job logs, Athena SQL, exported CSV results |
| Version Control       | GitHub                                          |

## Source Data

The pipeline processes three primary source tables:

| Source Table         | Description                                             |
| -------------------- | ------------------------------------------------------- |
| `order_items`        | Restaurant order line-item data                         |
| `order_item_options` | Item modifiers/options associated with order line items |
| `date_dim`           | Calendar/date dimension data                            |

The original source files were loaded into SQL Server, and AWS Glue jobs then extracted the data from SQL Server into S3.

## Architecture

### High-Level Flow

```text
SQL Server
    ↓
AWS Glue ingestion jobs
    ↓
S3 raw layer
    ↓
AWS Glue silver cleansing jobs
    ↓
S3 silver layer
    ↓
AWS Glue gold modeling jobs
    ↓
S3 gold layer
    ↓
AWS Glue mart transformation jobs
    ↓
S3 marts layer
    ↓
AWS Glue crawler
    ↓
Glue Data Catalog / Athena
    ↓
Streamlit dashboard
```

### S3 Layering Strategy

The S3 bucket uses a layered structure:

```text
s3://dea-restaurant-biz-insights/
├── raw/
├── silver/
├── gold/
├── marts/
├── scripts/
├── logs/
└── athena-query-results/
```

Each output layer is partitioned by `ingest_date`, for example:

```text
s3://dea-restaurant-biz-insights/marts/daily_sales/ingest_date=2026-05-27/
```

The project originally wrote `ingest_date` both as a physical Parquet column and as an S3 partition folder. This caused duplicate-column metadata errors in Athena. The pipeline was refactored so that `ingest_date` is retained as an S3 partition key, but removed from the physical Parquet files before writing.

## Pipeline Layers

### Raw Layer

The raw layer stores data extracted from SQL Server with minimal transformation.

Raw ingestion jobs:

| Glue Job                                      | Output                   |
| --------------------------------------------- | ------------------------ |
| `restaurant_ingest_order_items_to_raw`        | `raw/order_items`        |
| `restaurant_ingest_order_item_options_to_raw` | `raw/order_item_options` |
| `restaurant_ingest_date_dim_to_raw`           | `raw/date_dim`           |

### Silver Layer

The silver layer performs cleansing, type normalization, validation, and business-rule preparation.

Silver jobs:

| Glue Job                                     | Output                            |
| -------------------------------------------- | --------------------------------- |
| `restaurant_silver_order_items_clean`        | `silver/order_items_clean`        |
| `restaurant_silver_order_item_options_clean` | `silver/order_item_options_clean` |
| `restaurant_silver_date_dim_clean`           | `silver/date_dim_clean`           |

Silver-layer responsibilities include:

* Standardizing data types
* Cleaning invalid or malformed keys
* Casting numeric fields
* Validating row counts
* Preserving relevant source-system behavior such as repeated options/modifiers
* Preparing clean data for gold transformations

### Gold Layer

The gold layer creates conformed analytical fact tables.

Gold jobs:

| Glue Job                          | Output                 |
| --------------------------------- | ---------------------- |
| `restaurant_gold_fact_order_line` | `gold/fact_order_line` |
| `restaurant_gold_fact_order`      | `gold/fact_order`      |

Gold outputs include:

* Order-line-level revenue
* Option revenue attached to order lines
* Order-level aggregated facts
* Customer/order attribution fields
* Revenue components such as gross item revenue, option revenue, discounts, and net revenue

### Mart Layer

The mart layer creates business-ready analytical tables.

Mart jobs:

| Glue Job                                 | Mart Table               | Description                                        |
| ---------------------------------------- | ------------------------ | -------------------------------------------------- |
| `restaurant_mart_customer_daily_clv`     | `customer_daily_clv`     | Customer daily spend and cumulative CLV            |
| `restaurant_mart_customer_clv_snapshot`  | `customer_clv_snapshot`  | One-row-per-customer CLV snapshot                  |
| `restaurant_mart_customer_rfm`           | `customer_rfm`           | Customer recency, frequency, monetary segmentation |
| `restaurant_mart_daily_sales`            | `daily_sales`            | Business-wide daily sales metrics                  |
| `restaurant_mart_restaurant_daily_sales` | `restaurant_daily_sales` | Restaurant-level daily sales metrics               |
| `restaurant_mart_item_sales`             | `item_sales`             | Menu item performance metrics                      |
| `restaurant_mart_restaurant_item_sales`  | `restaurant_item_sales`  | Restaurant/item combination performance            |
| `restaurant_mart_option_sales`           | `option_sales`           | Modifier/option performance metrics                |

## Glue Workflow Orchestration

The pipeline is orchestrated with an AWS Glue Workflow.

The workflow starts with the raw ingestion jobs, proceeds through silver jobs, then gold jobs, then mart jobs. Conditional Glue triggers ensure downstream jobs only run after required upstream jobs succeed.

The workflow concludes with a Glue crawler:

```text
restaurant-marts-crawler
```

The crawler updates the Glue Data Catalog database so that Athena can query the latest mart outputs.

A generated workflow documentation file is included in the repository:

[Glue workflow orchestration documentation](docs/workflow_orchestration.md)

The exported Glue workflow JSON is also included as supporting evidence.

## Glue Data Catalog and Athena Integration

After the Glue crawler runs, the S3 mart outputs are registered as tables in the AWS Glue Data Catalog database `restaurant_analytics`.

Athena then queries those Glue Catalog tables directly.

Glue Data Catalog database:

```text
restaurant_analytics
```

Primary mart tables:

```text
customer_clv_snapshot
customer_daily_clv
customer_rfm
daily_sales
item_sales
option_sales
restaurant_daily_sales
restaurant_item_sales
```

The PySpark transformation logic that creates the Silver, Gold, and Mart datasets is stored in the project repository under:

```text
glue_jobs/
```

Athena was used to validate the final mart outputs and run business analysis queries against the curated tables. Those validation and analysis SQL files are stored in the project repository under:

```text
sql/athena/
```


## Validation Strategy

The pipeline was validated by comparing:

1. Glue job log output
2. Athena validation query results
3. Exported Athena CSV result files
4. Expected row counts and revenue totals across marts

Validation evidence is stored under:

```text
docs/validation/
├── athena_results/
├── glue_logs/
└── pipeline_validation_summary.md
```

Supporting validation documentation:

- [Pipeline validation summary](docs/validation/pipeline_validation_summary.md)
- [Athena validation results](docs/validation/athena_results/)
- [Glue job logs](docs/validation/glue_logs/)

### Validation Summary

The following high-level validation checks passed:

| Mart                     |           Metric |      Glue Log | Athena Result | Status |
| ------------------------ | ---------------: | ------------: | ------------: | ------ |
| `daily_sales`            |     total_orders |       131,328 |       131,328 | Match  |
| `daily_sales`            |      net_revenue | 10,018,999.83 | 10,018,999.83 | Match  |
| `restaurant_daily_sales` |     total_orders |       131,328 |       131,328 | Match  |
| `restaurant_daily_sales` |      net_revenue | 10,018,999.83 | 10,018,999.83 | Match  |
| `item_sales`             |       line_items |       203,519 |       203,519 | Match  |
| `item_sales`             |    item_quantity |       227,487 |       227,487 | Match  |
| `item_sales`             |      net_revenue | 10,018,999.83 | 10,018,999.83 | Match  |
| `restaurant_item_sales`  |       line_items |       203,519 |       203,519 | Match  |
| `restaurant_item_sales`  |    item_quantity |       227,487 |       227,487 | Match  |
| `restaurant_item_sales`  |      net_revenue | 10,018,999.83 | 10,018,999.83 | Match  |
| `customer_clv_snapshot`  |        customers |        20,174 |        20,174 | Match  |
| `customer_clv_snapshot`  | lifetime_revenue |  7,777,702.34 |  7,777,702.34 | Match  |
| `customer_rfm`           |        customers |        20,174 |        20,174 | Match  |
| `customer_rfm`           | lifetime_revenue |  7,777,702.34 |  7,777,702.34 | Match  |
| `option_sales`           |      option_rows |       193,017 |       193,017 | Match  |
| `option_sales`           |  option_quantity |       193,017 |       193,017 | Match  |
| `option_sales`           |   option_revenue |     85,269.14 |     85,269.14 | Match  |

### Known Data Quality Findings

The following data-quality findings were identified and documented:

1. `date_dim_clean` covers 2023 only, while `fact_order` spans 2020-04-21 through 2024-02-20. The daily sales marts derive fallback date attributes for unmatched dates.

2. `order_item_options_clean` contains 616 duplicate candidate option key groups and 2,299 duplicate candidate option key rows. These were preserved because they may represent legitimate repeated options/modifiers.

3. `fact_order_line` reported 15 unmatched option line-item keys. This explains the $24.00 difference between global `option_sales` option revenue and option revenue attached to matched order lines.

These findings were treated as data-quality observations rather than pipeline failures.

## Streamlit Dashboard

A separate Streamlit dashboard repository was created for the presentation layer.

[Restaurant Business Insights Dashboard Repo](https://github.com/guymandev/dea-restaurant-biz-insights-dashboard)

The dashboard connects to Athena using:

* `awswrangler`
* `boto3`
* Streamlit secrets
* A narrowly scoped IAM user for dashboard access

The deployed dashboard queries the Athena mart tables directly.

Link to deployed Streamlit dashboard:

[Restaurant Business Insights Dashboard](https://dea-restaurant-biz-insights-dashboard-pf2eonu4wgytgnejaxfupy.streamlit.app/)

### Dashboard Pages

| Page                   | Description                                                          |
| ---------------------- | -------------------------------------------------------------------- |
| Home                   | Project overview and Athena connection check                         |
| Executive Overview     | Total revenue, orders, customers, AOV, and daily trends              |
| Restaurant Performance | Restaurant-level revenue, order volume, AOV, and daily sales trends  |
| Menu Item Performance  | Item revenue, category performance, and restaurant/item combinations |
| Customer Analytics     | CLV tiers, RFM segmentation, and customer-level behavior             |
| Option Analytics       | Modifier/option revenue, quantity, and option group performance      |

### Streamlit Architecture

```text
Streamlit Cloud
    ↓
awswrangler / boto3
    ↓
Amazon Athena
    ↓
Glue Data Catalog
    ↓
S3 mart Parquet files
```

### Streamlit Secrets

The deployed app uses Streamlit secrets in TOML format:

```toml
[aws]
aws_access_key_id = "..."
aws_secret_access_key = "..."
region_name = "us-east-2"
athena_database = "restaurant_analytics"
athena_output_location = "s3://dea-restaurant-biz-insights/athena-query-results/"
```

Secrets are not committed to GitHub.

## Security Notes

The Streamlit dashboard uses a dedicated IAM user with limited permissions for:

* Athena query execution
* Glue Data Catalog read access
* S3 read access to mart outputs
* S3 write/read access to Athena query results
* KMS decrypt/generate data key permissions for encrypted S3 resources

This avoids reusing unrelated project credentials and keeps dashboard access scoped to the restaurant analytics project.

## Key Business Insights

The validated mart outputs support the following observations:

* Total net revenue across the pipeline is `$10,018,999.83`.
* Total order count is `131,328`.
* The customer CLV marts include `20,174` customers.
* Customer-attributed lifetime revenue is `$7,777,702.34`.
* The item sales marts include `203,519` line items and `227,487` item quantity.
* The option sales mart includes `193,017` option rows and `$85,269.14` in option revenue.
* The highest-revenue menu item is `Korean Kimchi`.
* The highest-revenue option is `Add Smokehouse Bacon`.
* The highest-quantity option is `Add Almond Milk`.
* Customer CLV tiers were recalculated at the customer snapshot level to produce balanced customer segmentation.

## Project Challenges and Resolutions

### Duplicate `ingest_date` Columns in Athena

Initial Athena queries failed because the crawler detected `ingest_date` both as a physical Parquet column and as an S3 partition column.

Resolution:

* Refactored Glue jobs to keep `ingest_date` in memory for validation/logging.
* Dropped `ingest_date` before writing partitioned Parquet files.
* Preserved `ingest_date` as the S3 partition folder.
* Reran the Glue workflow and crawler.
* Confirmed Athena schemas only include `ingest_date` as a partition key.

### Date Dimension Coverage

The source date dimension only covered 2023, while order data ranged from 2020 to 2024.

Resolution:

* Daily sales marts preserve the available `date_dim_clean` attributes when present.
* For unmatched dates, fallback date attributes are derived from `order_date`.
* The issue is documented as a data-quality finding.

### CLV Quartile Assignment

Initial CLV tiering was inherited from daily rows, which created uneven customer-level tier distributions.

Resolution:

* Refactored `customer_clv_snapshot` to calculate CLV quartiles after selecting one latest row per customer.
* This produced balanced customer segmentation across high, medium-high, medium-low, and low CLV tiers.

### Workflow Documentation

The AWS Glue console graph was difficult to capture clearly in screenshots.

Resolution:

* Exported the Glue workflow JSON with the AWS CLI.
* Generated Markdown workflow documentation.
* Created a Mermaid workflow diagram and trigger dependency table.

## Repository Structure

```text
.
├── config/
├── docs/
│   ├── images/
│   ├── validation/
│   │   ├── athena_results/
│   │   ├── glue_logs/
│   │   └── pipeline_validation_summary.md
│   └── workflow_orchestration.md
├── glue_jobs/
├── scripts/
├── sql/
│   └── athena/
│       ├── business_analysis_queries.sql
│       └── mart_validation_queries.sql
├── .gitignore
├── initial_restaurant_analysis.py
└── README.md
```

The `initial_restaurant_analysis.py` script was used during the discovery phase to profile the source CSV files, including null checks, likely primary keys, cardinality, and early metric-readiness analysis.

## How to Run the Pipeline

### 1. Run the Glue Workflow

In AWS Glue, run:

```text
restaurant_daily_pipeline_workflow
```

The workflow executes ingestion, silver, gold, and mart jobs, then runs the marts crawler.

### 2. Confirm Workflow Completion

Verify that all jobs and the final crawler complete successfully.

### 3. Query the Mart Tables in Athena

Use database:

```text
restaurant_analytics
```

Example query:

```sql
SELECT
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS total_net_revenue
FROM restaurant_analytics.daily_sales
WHERE ingest_date = '2026-05-27';
```

Expected result:

```text
total_orders: 131328
total_net_revenue: 10018999.83
```

### 4. View the Dashboard

The Streamlit dashboard connects to Athena and displays the final business-facing analytics.

## Final Outcome

This project delivers a complete data engineering and analytics solution:

* SQL Server source ingestion
* Automated AWS Glue workflow orchestration
* S3-based raw, silver, gold, and mart layers
* Curated Parquet marts partitioned by ingest date
* Glue Data Catalog and Athena query access
* Validation evidence comparing Glue logs to Athena results
* Deployed Streamlit dashboard backed by Athena

The result is an end-to-end restaurant analytics platform that is scalable, queryable, documented, validated, and presentation-ready.
