"""
Initial Restaurant Capstone Data Analysis

Purpose:
    Profiles the three restaurant capstone CSV files, validates expected keys and
    relationships, checks metric readiness, and writes analysis reports to ./reports.

Expected source files:
    ./data/order_items.csv
    ./data/order_item_options.csv
    ./data/date_dim.csv

Outputs:
    ./reports/data_profile_summary_<timestamp>.csv
    ./reports/key_validation_<timestamp>.csv
    ./reports/relationship_validation_<timestamp>.csv
    ./reports/data_quality_checks_<timestamp>.csv
    ./reports/metric_readiness_<timestamp>.csv
    ./reports/profile_run_<timestamp>.log

Usage:
    python initial_restaurant_analysis.py

Optional:
    python initial_restaurant_analysis.py --data-dir ./data --reports-dir ./reports --download
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    import gdown
    #from gdown.download import download as gdown_download
except ImportError:  # Allows profiling local files even if gdown is not installed.
    gdown = None


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

GOOGLE_DRIVE_FILES = {
    "order_items.csv": "https://drive.google.com/uc?id=1GXRZNgfngU6Yal6hzs5NClDgJoN3vEKZ",
    "order_item_options.csv": "https://drive.google.com/uc?id=1l9anZqzpgTsQXe1ZTg-ihhn-9SBsa2H_",
    "date_dim.csv": "https://drive.google.com/uc?id=1v1rPl4nJp1B_nQmNm_Nrz2ZeBkppKRYh",
}

EXPECTED_FILES = list(GOOGLE_DRIVE_FILES.keys())

ID_COLUMNS = [
    "restaurant_id",
    "order_id",
    "user_id",
    "printed_card_number",
    "lineitem_id",
]

EXPECTED_KEYS = {
    "order_items.csv": ["order_id", "lineitem_id"],
    "order_item_options.csv": ["order_id", "lineitem_id", "option_group_name", "option_name"],
    "date_dim.csv": ["date_key"],
}

METRIC_REQUIREMENTS = {
    "Customer Lifetime Value (CLV)": {
        "description": "Total and daily cumulative customer spend.",
        "required_columns": {
            "order_items.csv": ["user_id", "order_id", "lineitem_id", "item_price", "item_quantity"],
            "order_item_options.csv": ["order_id", "lineitem_id", "option_price", "option_quantity"],
        },
    },
    "CLV Tiering": {
        "description": "High/medium/low value customer groups based on customer spend percentiles.",
        "required_columns": {
            "order_items.csv": ["user_id", "item_price", "item_quantity"],
            "order_item_options.csv": ["order_id", "lineitem_id", "option_price", "option_quantity"],
        },
    },
    "RFM Segmentation": {
        "description": "Recency, frequency, and monetary segmentation.",
        "required_columns": {
            "order_items.csv": ["user_id", "order_id", "creation_time_utc", "item_price", "item_quantity"],
        },
    },
    "Churn Risk Indicators": {
        "description": "Days since last order, average gap between orders, and spend trend indicators.",
        "required_columns": {
            "order_items.csv": ["user_id", "order_id", "creation_time_utc", "item_price", "item_quantity"],
        },
    },
    "Sales Trends and Seasonality": {
        "description": "Daily, weekly, monthly, category, location, and holiday sales summaries.",
        "required_columns": {
            "order_items.csv": ["creation_time_utc", "restaurant_id", "item_category", "item_price", "item_quantity"],
            "date_dim.csv": ["date_key", "week", "month", "year", "is_weekend", "is_holiday"],
        },
    },
    "Loyalty Program Impact": {
        "description": "Comparison of spend, repeat ordering, and CLV for loyalty vs non-loyalty customers.",
        "required_columns": {
            "order_items.csv": ["user_id", "order_id", "is_loyalty", "item_price", "item_quantity"],
        },
    },
    "Location Performance": {
        "description": "Revenue, order volume, average order value, and ranking by restaurant location.",
        "required_columns": {
            "order_items.csv": ["restaurant_id", "order_id", "creation_time_utc", "item_price", "item_quantity"],
        },
    },
    "Pricing and Discount Effectiveness": {
        "description": "Discounted vs non-discounted revenue and order comparison.",
        "required_columns": {
            "order_items.csv": ["order_id", "lineitem_id", "item_price", "item_quantity"],
            "order_item_options.csv": ["order_id", "lineitem_id", "option_price", "option_quantity"],
        },
    },
}


# -----------------------------------------------------------------------------
# Logging helper
# -----------------------------------------------------------------------------

class Tee:
    """Duplicate stdout to multiple streams, e.g. terminal + log file."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()


@dataclass
class AnalysisContext:
    data_dir: Path
    reports_dir: Path
    timestamp: str


# -----------------------------------------------------------------------------
# File ingestion
# -----------------------------------------------------------------------------

def download_source_files(data_dir: Path, overwrite: bool = False) -> None:
    """Download expected CSV files from Google Drive using gdown."""
    if gdown is None:
        raise ImportError("gdown is not installed. Run: pip install gdown")

    data_dir.mkdir(parents=True, exist_ok=True)

    for filename, url in GOOGLE_DRIVE_FILES.items():
        output_path = data_dir / filename
        if output_path.exists() and not overwrite:
            print(f"Skipping existing file: {output_path}")
            continue

        print(f"Downloading {filename}...")

        download_fn = getattr(gdown, "download", None)
        if download_fn is None:
            raise AttributeError(
                "Installed gdown package does not expose a download() function. "
                "Try: pip install --upgrade gdown"
            )

        download_fn(url, str(output_path), quiet=False)


def read_csv_safely(path: Path, **kwargs) -> pd.DataFrame:
    """
    Read CSV with UTF-8 / CP1252 fallback and safe defaults for ID columns.
    For analysis, IDs are read as string to prevent accidental numeric coercion,
    dropped leading zeros, or inconsistent join behavior.

    Column names are normalized to lowercase snake_case so downstream validation
    logic can refer to consistent names like order_id, lineitem_id, item_price, etc.
    """
    try:
        cols = pd.read_csv(path, encoding="utf-8", nrows=0).columns.tolist()
        encoding = "utf-8"
    except UnicodeDecodeError:
        cols = pd.read_csv(path, encoding="cp1252", nrows=0).columns.tolist()
        encoding = "cp1252"

    caller_dtype = kwargs.pop("dtype", {}) or {}

    # Match ID columns case-insensitively, because the raw CSVs use uppercase names.
    normalized_id_cols = {c.lower() for c in ID_COLUMNS}
    default_dtype = {
        col: "string"
        for col in cols
        if col.strip().lower() in normalized_id_cols
    }

    default_dtype.update(caller_dtype)

    df = pd.read_csv(path, encoding=encoding, dtype=default_dtype, **kwargs)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    return df


def load_dataframes(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Load expected CSV files into a dictionary keyed by filename."""
    dfs = {}
    for filename in EXPECTED_FILES:
        path = data_dir / filename
        if not path.exists():
            print(f"WARNING: Missing expected file: {path}")
            continue
        dfs[filename] = read_csv_safely(path)
    return dfs


# -----------------------------------------------------------------------------
# Generic profiling
# -----------------------------------------------------------------------------

def show_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.dtypes.reset_index().rename(columns={"index": "column", 0: "dtype"})


def summarize_file(filename: str, df: pd.DataFrame) -> dict:
    nrows, ncols = df.shape
    null_pct = (df.isna().mean() * 100).sort_values(ascending=False)
    nunique = df.nunique(dropna=True).sort_values(ascending=False)
    duplicate_rows = int(df.duplicated().sum())

    return {
        "file": filename,
        "rows": int(nrows),
        "cols": int(ncols),
        "duplicate_full_rows": duplicate_rows,
        "top_null_cols": "; ".join([f"{col}={pct:.2f}%" for col, pct in null_pct.head(8).items()]),
        "top_cardinality_cols": "; ".join([f"{col}={int(val)}" for col, val in nunique.head(8).items()]),
    }


def print_file_profile(filename: str, df: pd.DataFrame) -> None:
    nrows, ncols = df.shape
    null_pct = (df.isna().mean() * 100).sort_values(ascending=False)
    nunique = df.nunique(dropna=True).sort_values(ascending=False)

    print("\n" + "=" * 90)
    print(f"FILE: {filename}")
    print(f"SHAPE: {nrows:,} rows x {ncols:,} columns")

    print("\nSCHEMA:")
    print(show_schema(df).to_string(index=False))

    print("\nTOP NULL %:")
    print(null_pct.head(15).round(3).to_string())

    print("\nTOP CARDINALITY:")
    print(nunique.head(15).to_string())

    print("\nSAMPLE ROWS:")
    print(df.head(5).to_string(index=False))


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------

def missing_columns(df: pd.DataFrame, required_cols: Iterable[str]) -> list[str]:
    return [col for col in required_cols if col not in df.columns]


def validate_key(filename: str, df: pd.DataFrame, key_cols: list[str]) -> dict:
    missing = missing_columns(df, key_cols)
    if missing:
        return {
            "file": filename,
            "candidate_key": "|".join(key_cols),
            "status": "FAIL",
            "row_count": int(len(df)),
            "unique_key_count": None,
            "duplicate_key_count": None,
            "null_key_rows": None,
            "uniqueness_ratio": None,
            "notes": f"Missing key columns: {missing}",
        }

    row_count = len(df)
    unique_key_count = df.drop_duplicates(subset=key_cols).shape[0]
    duplicate_key_count = int(df.duplicated(subset=key_cols).sum())
    null_key_rows = int(df[key_cols].isna().any(axis=1).sum())
    uniqueness_ratio = round(unique_key_count / row_count, 6) if row_count else None

    status = "PASS" if duplicate_key_count == 0 and null_key_rows == 0 else "WARN"
    notes = []
    if duplicate_key_count:
        notes.append(f"{duplicate_key_count:,} duplicate key rows")
    if null_key_rows:
        notes.append(f"{null_key_rows:,} rows with null key component")
    if not notes:
        notes.append("Expected key is unique and complete")

    return {
        "file": filename,
        "candidate_key": "|".join(key_cols),
        "status": status,
        "row_count": int(row_count),
        "unique_key_count": int(unique_key_count),
        "duplicate_key_count": duplicate_key_count,
        "null_key_rows": null_key_rows,
        "uniqueness_ratio": uniqueness_ratio,
        "notes": "; ".join(notes),
    }


def check_numeric_column(df: pd.DataFrame, filename: str, col: str) -> dict:
    if col not in df.columns:
        return {
            "file": filename,
            "check_name": f"numeric_{col}",
            "status": "FAIL",
            "affected_rows": None,
            "notes": f"Missing column: {col}",
        }

    coerced = pd.to_numeric(df[col], errors="coerce")
    non_null_original = df[col].notna()
    invalid = non_null_original & coerced.isna()

    status = "PASS" if int(invalid.sum()) == 0 else "FAIL"
    return {
        "file": filename,
        "check_name": f"numeric_{col}",
        "status": status,
        "affected_rows": int(invalid.sum()),
        "notes": "All non-null values are numeric" if status == "PASS" else "Some non-null values could not be coerced to numeric",
    }


def check_non_positive(df: pd.DataFrame, filename: str, col: str, allow_negative: bool = False) -> list[dict]:
    if col not in df.columns:
        return [{
            "file": filename,
            "check_name": f"non_positive_{col}",
            "status": "FAIL",
            "affected_rows": None,
            "notes": f"Missing column: {col}",
        }]

    numeric = pd.to_numeric(df[col], errors="coerce")
    zero_count = int((numeric == 0).sum())
    negative_count = int((numeric < 0).sum())

    results = [{
        "file": filename,
        "check_name": f"zero_{col}",
        "status": "WARN" if zero_count else "PASS",
        "affected_rows": zero_count,
        "notes": f"Rows where {col} is zero",
    }]

    results.append({
        "file": filename,
        "check_name": f"negative_{col}",
        "status": "PASS" if allow_negative or negative_count == 0 else "WARN",
        "affected_rows": negative_count,
        "notes": (
            f"Rows where {col} is negative. Negative values may be expected for discounts."
            if allow_negative else f"Rows where {col} is negative"
        ),
    })

    return results


# -----------------------------------------------------------------------------
# Restaurant-specific validations
# -----------------------------------------------------------------------------

def validate_relationships(dfs: dict[str, pd.DataFrame]) -> list[dict]:
    results = []

    order_items = dfs.get("order_items.csv")
    options = dfs.get("order_item_options.csv")
    date_dim = dfs.get("date_dim.csv")

    # order_item_options -> order_items on order_id + lineitem_id
    if order_items is not None and options is not None:
        join_cols = ["order_id", "lineitem_id"]
        missing_left = missing_columns(options, join_cols)
        missing_right = missing_columns(order_items, join_cols)

        if missing_left or missing_right:
            results.append({
                "relationship": "order_item_options -> order_items",
                "join_columns": "order_id|lineitem_id",
                "status": "FAIL",
                "left_rows": len(options),
                "matched_rows": None,
                "unmatched_rows": None,
                "unmatched_pct": None,
                "notes": f"Missing columns. options={missing_left}, order_items={missing_right}",
            })
        else:
            parent_keys = order_items[join_cols].drop_duplicates()
            merged = options[join_cols].merge(parent_keys, on=join_cols, how="left", indicator=True)
            unmatched = int((merged["_merge"] == "left_only").sum())
            total = len(options)
            results.append({
                "relationship": "order_item_options -> order_items",
                "join_columns": "order_id|lineitem_id",
                "status": "PASS" if unmatched == 0 else "WARN",
                "left_rows": int(total),
                "matched_rows": int(total - unmatched),
                "unmatched_rows": unmatched,
                "unmatched_pct": round((unmatched / total) * 100, 4) if total else None,
                "notes": "Every option row maps to an order item" if unmatched == 0 else "Some option rows do not map to an order item",
            })

    # order_items.creation_time_utc date -> date_dim.date_key
    if order_items is not None and date_dim is not None:
        required_order_cols = ["creation_time_utc"]
        required_date_cols = ["date_key"]
        missing_order = missing_columns(order_items, required_order_cols)
        missing_date = missing_columns(date_dim, required_date_cols)

        if missing_order or missing_date:
            results.append({
                "relationship": "order_items order_date -> date_dim",
                "join_columns": "DATE(creation_time_utc)|date_key",
                "status": "FAIL",
                "left_rows": len(order_items),
                "matched_rows": None,
                "unmatched_rows": None,
                "unmatched_pct": None,
                "notes": f"Missing columns. order_items={missing_order}, date_dim={missing_date}",
            })

        else:
            
            order_dates = pd.to_datetime(
                order_items["creation_time_utc"], 
                errors="coerce", 
                utc=True).dt.date
            
            dim_dates = pd.to_datetime(
                date_dim["date_key"],
                format="%d-%m-%Y",
                errors="coerce"
            ).dt.date.dropna().drop_duplicates()

            dim_date_set = set(dim_dates)
            valid_order_dates = order_dates.dropna()
            unmatched = int((~valid_order_dates.isin(dim_date_set)).sum())
            total = int(valid_order_dates.shape[0])
            invalid_timestamp_count = int(order_dates.isna().sum())

            status = "PASS" if unmatched == 0 and invalid_timestamp_count == 0 else "WARN"
            results.append({
                "relationship": "order_items order_date -> date_dim",
                "join_columns": "DATE(creation_time_utc)|date_key",
                "status": status,
                "left_rows": int(len(order_items)),
                "matched_rows": int(total - unmatched),
                "unmatched_rows": unmatched,
                "unmatched_pct": round((unmatched / total) * 100, 4) if total else None,
                "notes": f"Invalid timestamps: {invalid_timestamp_count:,}; unmatched valid dates: {unmatched:,}",
            })

    return results


def validate_data_quality(dfs: dict[str, pd.DataFrame]) -> list[dict]:

    results = []

    order_items = dfs.get("order_items.csv")
    options = dfs.get("order_item_options.csv")
    date_dim = dfs.get("date_dim.csv")

    if order_items is not None:

        for col in ["item_price", "item_quantity"]:

            results.append(check_numeric_column(order_items, "order_items.csv", col))
            results.extend(check_non_positive(order_items, "order_items.csv", col, allow_negative=False))

        for col in ["user_id", "restaurant_id", "order_id", "lineitem_id", "creation_time_utc"]:

            if col in order_items.columns:

                missing_count = int(order_items[col].isna().sum())

                results.append({
                    "file": "order_items.csv",
                    "check_name": f"missing_{col}",
                    "status": "PASS" if missing_count == 0 else "WARN",
                    "affected_rows": missing_count,
                    "notes": f"Rows where {col} is missing",
                })

        if "creation_time_utc" in order_items.columns:

            parsed = pd.to_datetime(order_items["creation_time_utc"], errors="coerce", utc=True)
            invalid = int(parsed.isna().sum())
            results.append({
                "file": "order_items.csv",
                "check_name": "valid_creation_time_utc",
                "status": "PASS" if invalid == 0 else "WARN",
                "affected_rows": invalid,
                "notes": "Rows with invalid or missing timestamps",
            })

            if parsed.notna().any():

                results.append({
                    "file": "order_items.csv",
                    "check_name": "order_date_range",
                    "status": "INFO",
                    "affected_rows": int(parsed.notna().sum()),
                    "notes": f"Date range: {parsed.min()} to {parsed.max()}",
                })

        if "currency" in order_items.columns:

            currencies = order_items["currency"].dropna().astype(str).value_counts()

            results.append({
                "file": "order_items.csv",
                "check_name": "currency_distribution",
                "status": "INFO" if len(currencies) <= 1 else "WARN",
                "affected_rows": int(currencies.sum()),
                "notes": "; ".join([f"{idx}={val:,}" for idx, val in currencies.items()]),
            })

        if "is_loyalty" in order_items.columns:

            loyalty_counts = order_items["is_loyalty"].dropna().astype(str).value_counts()

            results.append({
                "file": "order_items.csv",
                "check_name": "loyalty_distribution",
                "status": "INFO",
                "affected_rows": int(loyalty_counts.sum()),
                "notes": "; ".join([f"{idx}={val:,}" for idx, val in loyalty_counts.items()]),
            })

    if options is not None:

        for col in ["option_price", "option_quantity"]:

            results.append(check_numeric_column(options, "order_item_options.csv", col))
            results.extend(check_non_positive(options, "order_item_options.csv", col, allow_negative=(col == "option_price")))

        if "option_price" in options.columns:

            option_price = pd.to_numeric(options["option_price"], errors="coerce")
            discount_rows = int((option_price < 0).sum())

            results.append({
                "file": "order_item_options.csv",
                "check_name": "discount_option_rows",
                "status": "INFO" if discount_rows else "WARN",
                "affected_rows": discount_rows,
                "notes": "Rows where option_price < 0. These likely represent discounts/promotions.",
            })

    if date_dim is not None:

        if "date_key" in date_dim.columns:
            
            parsed = pd.to_datetime(
                date_dim["date_key"],
                format="%d-%m-%Y",
                errors="coerce"
            )
            
            invalid = int(parsed.isna().sum())
            duplicate_dates = int(date_dim.duplicated(subset=["date_key"]).sum())

            results.append({
                "file": "date_dim.csv",
                "check_name": "valid_date_key",
                "status": "PASS" if invalid == 0 and duplicate_dates == 0 else "WARN",
                "affected_rows": invalid + duplicate_dates,
                "notes": f"Invalid date_key rows: {invalid:,}; duplicate date_key rows: {duplicate_dates:,}",
            })

    return results


def build_metric_readiness(dfs: dict[str, pd.DataFrame]) -> list[dict]:
    rows = []

    for metric_name, config in METRIC_REQUIREMENTS.items():
        missing_items = []
        present_items = []

        for filename, required_cols in config["required_columns"].items():
            df = dfs.get(filename)
            if df is None:
                missing_items.append(f"{filename}: FILE MISSING")
                continue

            missing = missing_columns(df, required_cols)
            present = [col for col in required_cols if col in df.columns]

            if present:
                present_items.append(f"{filename}: {', '.join(present)}")
            if missing:
                missing_items.append(f"{filename}: {', '.join(missing)}")

        status = "READY" if not missing_items else "NOT_READY"
        rows.append({
            "metric_area": metric_name,
            "status": status,
            "description": config["description"],
            "present_columns": " | ".join(present_items),
            "missing_columns": " | ".join(missing_items),
            "notes": "All required source columns are present" if status == "READY" else "One or more required source columns are missing",
        })

    # Add relationship-driven readiness notes.
    if "order_items.csv" in dfs and "order_item_options.csv" in dfs:
        order_items = dfs["order_items.csv"]
        options = dfs["order_item_options.csv"]
        if all(col in order_items.columns for col in ["order_id", "lineitem_id"]) and all(col in options.columns for col in ["order_id", "lineitem_id"]):
            rows.append({
                "metric_area": "Revenue Enrichment Relationship",
                "status": "READY",
                "description": "Ability to attach options, modifiers, and discounts to order items.",
                "present_columns": "order_items.csv: order_id, lineitem_id | order_item_options.csv: order_id, lineitem_id",
                "missing_columns": "",
                "notes": "Join columns for item-to-option enrichment are present. See relationship_validation report for orphan checks.",
            })

    return rows


# -----------------------------------------------------------------------------
# Optional lightweight business summaries
# -----------------------------------------------------------------------------

def print_business_snapshot(dfs: dict[str, pd.DataFrame]) -> None:
    order_items = dfs.get("order_items.csv")
    options = dfs.get("order_item_options.csv")

    if order_items is None:
        return

    print("\n" + "=" * 90)
    print("BUSINESS SNAPSHOT")

    def safe_nunique(col: str) -> str:
        return f"{order_items[col].nunique(dropna=True):,}" if col in order_items.columns else "column missing"

    print(f"Unique orders:      {safe_nunique('order_id')}")
    print(f"Unique customers:   {safe_nunique('user_id')}")
    print(f"Unique restaurants: {safe_nunique('restaurant_id')}")
    print(f"Unique categories:  {safe_nunique('item_category')}")

    if {"item_price", "item_quantity"}.issubset(order_items.columns):
        item_price = pd.to_numeric(order_items["item_price"], errors="coerce")
        item_qty = pd.to_numeric(order_items["item_quantity"], errors="coerce")
        gross_item_revenue = (item_price * item_qty).sum(skipna=True)
        print(f"Gross item revenue estimate: {gross_item_revenue:,.2f}")

    if options is not None and {"option_price", "option_quantity"}.issubset(options.columns):
        option_price = pd.to_numeric(options["option_price"], errors="coerce")
        option_qty = pd.to_numeric(options["option_quantity"], errors="coerce")
        option_revenue = (option_price * option_qty).sum(skipna=True)
        discount_value = (option_price[option_price < 0] * option_qty[option_price < 0]).sum(skipna=True)
        print(f"Net option revenue estimate: {option_revenue:,.2f}")
        print(f"Discount option value estimate: {discount_value:,.2f}")


# -----------------------------------------------------------------------------
# Report writer
# -----------------------------------------------------------------------------

def write_report(df: pd.DataFrame, ctx: AnalysisContext, base_name: str) -> Path:
    path = ctx.reports_dir / f"{base_name}_{ctx.timestamp}.csv"
    df.to_csv(path, index=False)
    print(f"✅ Wrote {base_name}: {path.resolve()}")
    return path


def run_analysis(data_dir: str = "./data", reports_dir: str = "./reports", download: bool = False, overwrite: bool = False) -> None:
    ctx = AnalysisContext(
        data_dir=Path(data_dir),
        reports_dir=Path(reports_dir),
        timestamp=datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
    ctx.reports_dir.mkdir(parents=True, exist_ok=True)

    if download:
        download_source_files(ctx.data_dir, overwrite=overwrite)

    dfs = load_dataframes(ctx.data_dir)

    if not dfs:
        raise FileNotFoundError(
            f"No expected CSV files found in {ctx.data_dir.resolve()}. "
            "Run with --download or place the CSV files in the data directory."
        )

    print(f"Pandas version: {pd.__version__}")
    print(f"Data directory: {ctx.data_dir.resolve()}")
    print(f"Reports directory: {ctx.reports_dir.resolve()}")

    # Generic file profiles.
    summary_rows = []
    for filename, df in dfs.items():
        print_file_profile(filename, df)
        summary_rows.append(summarize_file(filename, df))

    write_report(pd.DataFrame(summary_rows), ctx, "data_profile_summary")

    # Expected key validations.
    key_rows = []
    for filename, key_cols in EXPECTED_KEYS.items():
        df = dfs.get(filename)
        if df is None:
            key_rows.append({
                "file": filename,
                "candidate_key": "|".join(key_cols),
                "status": "FAIL",
                "row_count": None,
                "unique_key_count": None,
                "duplicate_key_count": None,
                "null_key_rows": None,
                "uniqueness_ratio": None,
                "notes": "Expected file is missing",
            })
        else:
            key_rows.append(validate_key(filename, df, key_cols))

    key_df = pd.DataFrame(key_rows)
    print("\n" + "=" * 90)
    print("KEY VALIDATION")
    print(key_df.to_string(index=False))
    write_report(key_df, ctx, "key_validation")

    # Relationship validation.
    relationship_df = pd.DataFrame(validate_relationships(dfs))
    print("\n" + "=" * 90)
    print("RELATIONSHIP VALIDATION")
    print(relationship_df.to_string(index=False) if not relationship_df.empty else "No relationship checks were run.")
    write_report(relationship_df, ctx, "relationship_validation")

    # Data quality checks.
    quality_df = pd.DataFrame(validate_data_quality(dfs))
    print("\n" + "=" * 90)
    print("DATA QUALITY CHECKS")
    print(quality_df.to_string(index=False) if not quality_df.empty else "No data quality checks were run.")
    write_report(quality_df, ctx, "data_quality_checks")

    # Metric readiness.
    readiness_df = pd.DataFrame(build_metric_readiness(dfs))
    print("\n" + "=" * 90)
    print("METRIC READINESS")
    print(readiness_df.to_string(index=False))
    write_report(readiness_df, ctx, "metric_readiness")

    print_business_snapshot(dfs)


# -----------------------------------------------------------------------------
# CLI entrypoint with full console log
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile restaurant capstone source data.")
    parser.add_argument("--data-dir", default="./data", help="Directory containing source CSV files.")
    parser.add_argument("--reports-dir", default="./reports", help="Directory where reports will be written.")
    parser.add_argument("--download", action="store_true", help="Download source files from Google Drive before profiling.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite downloaded files if they already exist.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = reports_dir / f"profile_run_{ts}.log"

    with open(log_path, "w", encoding="utf-8") as log_file:
        original_stdout = sys.stdout
        sys.stdout = Tee(original_stdout, log_file)
        try:
            run_analysis(
                data_dir=args.data_dir,
                reports_dir=args.reports_dir,
                download=args.download,
                overwrite=args.overwrite,
            )
        finally:
            sys.stdout = original_stdout

    print(f"\n✅ Wrote full console log to: {log_path.resolve()}")


if __name__ == "__main__":
    main()
