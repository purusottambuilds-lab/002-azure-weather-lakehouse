# 002 - Azure Real-Time Weather Data Lakehouse

## Overview
A production-grade, real-time data lakehouse implementing Medallion Architecture
(Bronze/Silver/Gold) that ingests live weather data for 4 Indian cities every hour
using a fully parameterized ADF pipeline with ForEach activity. Detects weather
anomalies against 7-day rolling baselines, calculates rolling window analytics
using PySpark Window functions, applies severity scoring, and loads curated data
into Azure SQL - automated hourly with email alerting on failure.

---

## Architecture
```
Open-Meteo Free API - 4 cities: Bhubaneswar, Delhi, Mumbai, Bangalore
           |
           v

ADF: pl_002_weather_lakehouse (Parameterized + ForEach)
  
  Parameters: city_configs (array), run_date, city_list
  
  ForEach (IterateCities) [parallel, all 4 cities simultaneously]
    Web Activity  : FetchCityWeather (dynamic URL per city lat/lon)
    Copy Data     : SaveToBronze -> bronze/{city}/{yyyy-MM-dd-HH}/weather_raw.json
  
  Databricks: nb_01_bronze_to_silver
    - Reads run_date + city_list via dbutils.widgets (ADF Base parameters)
    - Flattens nested hourly JSON arrays (explode + arrays_zip)
    - Anomaly detection vs 7-day rolling city baseline
    - APPENDS to Silver Delta (history accumulates)
    - Returns row count via dbutils.notebook.exit()
  
  Databricks: nb_02_silver_to_gold
    - Daily aggregations per city (avg/max/min temp, rain, wind)
    - 7-day rolling window (PySpark Window.partitionBy.orderBy.rowsBetween)
    - Weather severity scoring: NORMAL / MODERATE / SEVERE
    - OVERWRITES Gold Delta (always latest daily summary)
  
  Databricks: nb_03_gold_to_sql
    - JDBC write Gold Delta to Azure SQL dbo.weather_daily_summary
    - JDBC write Anomalies to Azure SQL dbo.weather_anomalies
  
  Web Activity: Gmail alert via Logic App (success + failure)
           |
           v

Azure SQL: db-weather
  dbo.weather_daily_summary  -- all cities, all days
  dbo.weather_anomalies      -- flagged anomaly records
```

---

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | Orchestration, Web Activity, ForEach, hourly trigger |
| Open-Meteo API | Free real-time weather - no key, no rate limits |
| Azure Data Lake Storage Gen2 | Bronze / Silver / Gold Medallion layers |
| Azure Databricks + PySpark | Transformation, anomaly detection, analytics |
| Delta Lake | Versioned storage, schema evolution, audit trail |
| PySpark Window Functions | 7-day rolling average and trend analytics |
| Azure SQL Database | BI-ready curated output |
| Azure Logic Apps | Email alerting on pipeline failure |

---

## Advanced Concepts
| Concept | Implementation |
|---------|---------------|
| Medallion Architecture | Bronze raw JSON -> Silver cleaned Delta -> Gold analytics Delta |
| ADF Web Activity | Direct REST API call from ADF - no ingestion code |
| Parameterized Pipeline | city_configs array - add new cities without code changes |
| ForEach Activity | Parallel processing of 4 cities in one pipeline run |
| ADF to Databricks Params | Base parameters pass run_date + city_list into notebooks |
| dbutils.widgets | Notebooks read ADF parameters dynamically at runtime |
| dbutils.notebook.exit() | Notebooks return row counts back to ADF |
| Anomaly Detection | PySpark vs 7-day rolling city baseline |
| Rolling Window Analytics | PySpark Window functions - 7-day avg temp and precipitation |
| Weather Severity Scoring | NORMAL / MODERATE / SEVERE per city per day |
| JDBC from Databricks | Direct Spark JDBC to Azure SQL - no ADF Copy needed |
| Delta Schema Evolution | mergeSchema=True - new API fields handled gracefully |

---

## Repository Structure
```
002-azure-weather-lakehouse/
├── notebooks/
|   ├── nb_01_bronze_to_silver.py
|   ├── nb_02_silver_to_gold.py
|   └── nb_03_gold_to_sql.py
├── adf-pipelines/
|   └── pl_002_weather_lakehouse.json
├── data/
|   └── sample_bhubaneswar_weather.json
├── docs/
|   ├── adf_pipeline_overview.png
|   ├── adf_foreach_4_cities.png
|   ├── bronze_container_cities.png
|   ├── silver_anomaly_output.png
|   ├── gold_rolling_window.png
|   ├── gold_severity_scores.png
|   └── sql_output_verified.png
└── README.md
```


---

## Contact
Purusottam Swain (***purusottam.builds@gmail.com***)
- ***[Upwork](https://www.upwork.com/freelancers/~017164fcff771e794c?mp_source=share)***
- ***[Fiverr](https://www.fiverr.com/purusottam_sn?public_mode=true)***

---