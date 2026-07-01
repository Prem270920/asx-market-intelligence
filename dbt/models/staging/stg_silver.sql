-- Staging model: reads Silver Parquet directly via DuckDB's Parquet scanner.
-- DuckDB can query partitioned Parquet directories natively — no separate
-- "load" step needed, unlike traditional warehouses (BigQuery, Snowflake).

SELECT
    ticker,
    sector,
    "Date"          AS trade_date,
    "Open"          AS open_price,
    "High"          AS high_price,
    "Low"           AS low_price,
    "Close"         AS close_price,
    "Volume"        AS volume,
    log_return,
    sma_20,
    sma_50,
    bb_upper,
    bb_lower,
    bb_width,
    rsi_14,
    volatility_30d
FROM read_parquet('/Users/prem/Downloads/projects/asx-market-intelligence/data/silver/**/*.parquet', hive_partitioning = true)
