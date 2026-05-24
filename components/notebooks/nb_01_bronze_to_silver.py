# Databricks notebook source
# ================================================================
# nb_01_bronze_to_silver
# Project_002: Real-Time Weather Data Lakehouse
# Purpose: Read bronze JSON for all cities, flatten hourly arrays, detect anomalies, append to silver Delta Lake
# Author: Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# cell-1: read parameters passed from ADF

dbutils.widgets.text(
    "run_date", ""
)  # ADF passed: @{formatDateTime(utcNow(), 'yyyy-MM-dd-HH')}
dbutils.widgets.text("city_list", "Bhubaneswar,Delhi,Mumbai,Bangalore")

run_date = dbutils.widgets.get("run_date")
city_list = dbutils.widgets.get("city_list").split(",")

# fallback for manual runs (when ADF has not passed run_date)
if not run_date:
    from datetime import datetime

    run_date = datetime.now().strftime("%Y-%m-%d-%H")

print(f"Run date/hour: {run_date}")
print(f"Cities: {city_list}")

# COMMAND ----------

# cell-2: storage configuration

from pyspark.sql.functions import (
    col,
    explode,
    arrays_zip,
    current_timestamp,
    lit,
    to_timestamp,
    when,
    avg,
    abs as spark_abs,
    date_sub,
    current_date,
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import window
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame
import json

storage_account_name = "saweatherps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

bronze_base = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
silver_path = (
    f"abfss://silver@{storage_account_name}.dfs.core.windows.net/weather_cleaned"
)

print(f"Storage configured: {storage_account_name}")

# COMMAND ----------

# cell-3: real bronze JSON for all cities

all_city_dfs = []

for city_name in city_list:
    city_name = city_name.strip()
    bronze_path = f"{bronze_base}/{city_name}/{run_date}/weather_raw.json"

    try:
        df_raw = spark.read.option("multiline", "true").json(bronze_path)

        df_hourly = df_raw.select(
            col("latitude"),
            col("longitude"),
            col("timezone"),
            explode(
                arrays_zip(
                    col("hourly.time"),
                    col("hourly.temperature_2m"),
                    col("hourly.precipitation"),
                    col("hourly.windspeed_10m"),
                    col("hourly.weathercode"),
                    col("hourly.relativehumidity_2m"),
                    col("hourly.apparent_temperature"),
                )
            ).alias("h"),
        ).select(
            col("latitude"),
            col("longitude"),
            col("timezone"),
            col("h.time").alias("datetime_str"),
            col("h.temperature_2m").alias("temperature_celsius"),
            col("h.precipitation").alias("precipitation_mm"),
            col("h.windspeed_10m").alias("windspeed_kmph"),
            col("h.weathercode").alias("weather_code"),
            col("h.relativehumidity_2m").alias("humidity_pct"),
            col("h.apparent_temperature").alias("feels_like_celsius"),
        )

        df_city = df_hourly.withColumn("city_name", lit(city_name)).withColumn(
            "ingested_at", current_timestamp()
        )

        all_city_dfs.append(df_city)
        print(f" {city_name}: {df_city.count()} hourly recordds loaded")

    except Exception as e:
        print(f" Warning: Could not read {city_name} -- {str(e)[:120]}")
        print(f" Skipping {city_name} for this run")

if not all_city_dfs:
    raise Exception(
        "No city data could be read. verify ADF ForEach ran successfully first."
    )

df_all = reduce(DataFrame.union, all_city_dfs)
print(f"Total records across all cities: {df_all.count()}")

# COMMAND ----------

# cell-4: Clean and Type-Cast

df_clean = (
    df_all.na.drop(subset=["temperature_celsius", "city_name"])
    .withColumn("temperature_celsius", col("temperature_celsius").cast(DoubleType()))
    .withColumn("precipitation_mm", col("precipitation_mm").cast(DoubleType()))
    .withColumn("windspeed_kmph", col("windspeed_kmph").cast(DoubleType()))
    .withColumn("humidity_pct", col("humidity_pct").cast(IntegerType()))
    .withColumn("feels_like_celsius", col("feels_like_celsius").cast(DoubleType()))
    .withColumn("weather_code", col("weather_code").cast(IntegerType()))
    .withColumn("datetime_ts", to_timestamp(col("datetime_str")))
)

print(f"Clean records: {df_clean.count()}")
display(df_clean.limit(5))

# COMMAND ----------

# cell-5: Weather Anomaly Detection

# Anomaly conditions:
#   TEMPERATURE_SPIKE: temp deviates > 5c from city 7-day rolling average
#   HIGH_WIND: windspeed > 80kmph
#   HEAVY_RAIN: precipitation > 50mm in one hour

try:
    df_existing = spark.read.format("delta").load(
        silver_path
    )  # Ensure 'silver_path' has a supported scheme like 'dbfs:', 's3a:', or 'abfss:'

    # calculate 7-day rolling average temperature per city
    city_avg = (
        df_existing.filter(col("datetime_ts") >= date_sub(current_date(), 7))
        .groupBy("city_name")
        .agg(avg("temperature_celsius").alias("rolling_7d_avg_temp"))
    )

    df_with_avg = df_clean.join(city_avg, on="city_name", how="left")

    df_anomaly = (
        df_with_avg.withColumn(
            "is_anomaly",
            when(
                (spark_abs(col("temperature_celsius") - col("rolling_7d_avg_temp")) > 5)
                | (col("windspeed_kmph") > 80)
                | (col("precipitation_mm") > 50),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "anomaly_reason",
            when(
                spark_abs(col("temperature_celsius") - col("rolling_7d_avg_temp")) > 5,
                lit("TEMPERATURE_SPIKE"),
            )
            .when(col("windspeed_kmph") > 80, lit("HIGH_WIND"))
            .when(col("precipitation_mm") > 50, lit("HEAVY_RAIN"))
            .otherwise(lit(None)),
        )
        .drop("rolling_7d_avg_temp")
    )

    anomaly_count = df_anomaly.filter(col("is_anomaly") == True).count()
    print(f"Anomalies detected this run: {anomaly_count}")

except Exception as e:
    print(f"First run or no baseline - Anomaly detection skipped: {str(e)[:80]}")

    df_anomaly = df_clean.withColumn("is_anomaly", lit(False)).withColumn(
        "anomaly_reason", lit(None).cast("string")
    )

# COMMAND ----------

# cell-6: append to silver Delta Lake

df_anomaly.write.format("delta").mode("append").option("mergeSchema", "true").save(
    silver_path
)

total_silver = spark.read.format("delta").load(silver_path).count()
print(f"Silver layer total records after this run: {total_silver}")
display(df_anomaly.groupBy("city_name", "is_anomaly").count())

# COMMAND ----------

# cell-7: return status ADF

exit_value = json.dumps(
    {
        "status": "SUCCESS",
        "rows_processed": df_anomaly.count(),
        "run_date": run_date,
        "cities": city_list,
    }
)

dbutils.notebook.exit(exit_value)

# COMMAND ----------
