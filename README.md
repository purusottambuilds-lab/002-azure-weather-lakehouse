# 002 - Azure Weather Data Lakehouse — Medallion Architecture

## Overview
Real-time data pipeline that ingests live weather data from a free public API
every hour, processes it through Medallion Architecture (Bronze/Silver/Gold)
using PySpark in Azure Databricks, and produces daily aggregated analytics —
fully automated via Azure Data Factory.


## Architecture
```
Open-Meteo API (free, no API key needed)
|
v
Azure Data Factory  <-- Hourly Trigger
|
v
BRONZE Layer — ADLS Gen2
Raw JSON stored as: /bronze/yyyy-MM-dd/weather_raw.json
|
v
Azure Databricks + PySpark (Bronze → Silver)

Flatten nested JSON arrays into rows
Clean nulls
Add ingestion timestamp
|
v
SILVER Layer — ADLS Gen2 (Delta format)
Cleaned hourly weather records
|
v
Azure Databricks + PySpark (Silver → Gold)
Daily aggregations per location
avg/max/min temperature
total precipitation
avg/max wind speed
|
v
GOLD Layer — ADLS Gen2 (Delta format)
Daily analytics ready for BI/reporting
```


## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | API ingestion & hourly orchestration |
| Open-Meteo API | Free real-time weather data (no key needed) |
| Azure Data Lake Storage Gen2 | Bronze / Silver / Gold layers |
| Azure Databricks + PySpark | Layer-by-layer transformations |
| Delta Lake | All layers stored as Delta format |
| Python / SQL | Scripting & querying |


## Key Features
- Hourly automated API ingestion via ADF Web Activity + Schedule Trigger
- Medallion Architecture: Bronze (raw) → Silver (clean) → Gold (analytics)
- JSON flattening using PySpark explode() + arrays_zip()
- Incremental Silver append mode — data accumulates over time
- Gold layer: daily summaries ready for BI/reporting consumption
- Error handling and monitoring via ADF Monitor


## Data Source
Open-Meteo (open-meteo.com)
Free, no API key, no rate limits
Location: Bhubaneswar, Odisha, India (lat: 20.29, lon: 85.82)


## Repository Structure
```
notebooks/      -- PySpark transformation scripts (.py)
01_bronze_to_silver.py
02_silver_to_gold.py
03_weather_insights.py
adf-pipelines/  -- ADF pipeline JSON definitions
docs/           -- Architecture screenshots
README.md       -- This file
```