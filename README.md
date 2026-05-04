 End-to-end data engineering portfolio project demonstrating Apache Spark, medallion architecture, dbt, Airflow, and Streamlit.
## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, yfinance, pandas |
| Bronze | Apache Spark / PySpark, Parquet |
| Silver | PySpark Window Functions (SMA, RSI, Bollinger Bands) |
| Gold | dbt, DuckDB |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| Infrastructure | Docker, docker-compose |

## Data

ASX50 historical OHLCV data (2022–present), partitioned by ticker in Parquet format.

## Project Structure
## Running Locally

```bash
# Install dependencies
pip install pyspark yfinance pandas pyarrow

# Fetch data
python3 ingestion/fetch_asx.py

# Run Bronze layer
python3 spark_jobs/bronze_ingest.py
```

---

*Built as part of Master of Data Science — Monash University*
