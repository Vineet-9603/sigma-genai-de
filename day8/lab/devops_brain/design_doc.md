# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant details, and processes it into clean, enriched, and aggregated layers for reporting purposes.

## Data Flow Diagram

```
+--------------------+       +--------------------+       +--------------------+       +--------------------+
| Source             |       | Bronze Layer        |       | Silver Layer       |       | Gold Layer         |
|                    |       |                    |       |                    |       |                    |
| TRANSACTIONS_CLEAN  | --->  | bronze_transactions| --->  | silver_transactions| --->  | gold_merchant_perf  |
| TRANSACTIONS_DIRTY   |       |                    |       |                    |       |                    |
| MERCHANTS          |       |                    |       |                    |       | gold_daily_summary  |
+--------------------+       +--------------------+       +--------------------+       +--------------------+
```

## Key Design Decisions
- **Layered Approach**: Separates raw data ingestion, data cleaning, and aggregation to ensure modularity and ease of maintenance.
- **Quality Flags**: Introduced in the Silver layer to flag transactions that failed quality checks.
- **Aggregation**: Computes merchant performance and daily summaries in the Gold layer for efficient querying.
- **Ingestion Timestamp**: Captures the time of data ingestion for tracking and debugging.

## Known Limitations
- **Single Source**: Currently only ingests data from `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY`. Adding more sources would require modifications.
- **Static Merchant Data**: Merchants data is loaded once and not updated. This could lead to stale merchant information.
- **Failure Handling**: Limited error handling in data loading functions; could be improved for robustness.
- **Performance**: Aggregations in the Gold layer may become slow with a large dataset.

## Dependencies
- **DuckDB**: Database engine for storing and querying data.
- **MERCHANTS**: List of merchant details used for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files for transactions.