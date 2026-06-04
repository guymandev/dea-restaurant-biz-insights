# Validation Summary

* Workflow completed successfully.
* Glue crawler refreshed the Data Catalog
* Athena queried all mart tables successfully.
* Known data-quality findings are documented. 

| Mart | Metric | Glue Log | Athena Result | Status |
|---|---:|---:|---:|---|
| daily_sales | total_orders | 131,328 | 131,328 | ✅ Match |
| daily_sales | net_revenue | 10,018,999.83 | 10,018,999.83 | ✅ Match |
| item_sales | line_items | 203,519 | 203,519 | ✅ Match |
| item_sales | net_revenue | 10,018,999.83 | 10,018,999.83 | ✅ Match |
| customer_clv_snapshot | customers | 20,174 | 20,174 | ✅ Match |
| customer_rfm | lifetime_revenue | 7,777,702.34 | 7,777,702.34 | ✅ Match |
| option_sales | option_revenue | 85,269.14 | 85,269.14 | ✅ Match |

## Known Data Quality Notes

- `date_dim_clean` covers 2023 only, while `fact_order` spans 2020-04-21 through 2024-02-20. The daily sales marts derive fallback date attributes for unmatched dates.
- `order_item_options_clean` contains 616 duplicate candidate option key groups and 2,299 duplicate candidate option key rows. These were preserved because they may represent legitimate repeated options/modifiers.
- `fact_order_line` reported 15 unmatched option line-item keys. This explains the $24.00 difference between global `option_sales` option revenue and option revenue attached to matched order lines.