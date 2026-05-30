# Databricks notebook source
# ================================================================
# nb_01_bronze_to_silver
# Project 002   : Real-Time Weather Data Lakehouse
# Purpose       : Read bronze JSON for all cities, flatten hourly arrays, detect anomalies, append to Silver Delta Lake
# Folder        : dir_002_weather_lakehouse
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Read Parameters Passed from ADF

dbutils.widgets.text(
    "run_date", ""
)  # ADF passes: @{formatDateTime(utcNow(),'yyyy-MM-dd-HH')}
dbutils.widgets.text("city_list", "Bhubaneswar,Delhi,Mumbai,Bangalore")

run_date = dbutils.widgets.get("run_date")
city_list = dbutils.widgets.get("city_list").split(",")

# Fallback for manual runs — ADF always provides run_date so fallback never triggers in pipeline
if not run_date:
    from datetime import datetime

    run_date = datetime.now().strftime("%Y-%m-%d-%H")

print(f"Run date/hour : {run_date}")
print(f"Cities        : {city_list}")


# COMMAND ----------

# CELL 2 - Storage Configuration

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
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import Window
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame

storage_account_name = "saweatherps01"
storage_account_key = "YOUR_STORAGE_ACCOUNT_KEY_HERE"

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

# CELL 3 - Read Bronze JSON for All Cities

# ADF ForEach saves bronze files at:
#       bronze/{city_name}/{yyyy-MM-dd-HH}/weather_raw.json

all_city_dfs = []

for city_name in city_list:
    city_name = city_name.strip()
    bronze_path = f"{bronze_base}/{city_name}/{run_date}/weather_raw.json"

    try:
        df_raw = spark.read.option("multiline", "true").json(bronze_path)

        # Flatten nested hourly arrays into individual rows
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
        print(f"  {city_name}: {df_city.count()} hourly records loaded")

    except Exception as e:
        print(f"  WARNING: Could not read {city_name} -- {str(e)[:120]}")
        print(f"  Skipping {city_name} for this run")

if not all_city_dfs:
    raise Exception(
        "No city data could be read. Verify ADF ForEach ran successfully first."
    )

df_all = reduce(DataFrame.union, all_city_dfs)
print(f"Total records across all cities: {df_all.count()}")


# COMMAND ----------

# CELL 4 - Clean and Type-Cast

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

# CELL 5 - Weather Anomaly Detection

# TEMPERATURE_SPIKE : temp deviates > 5C from city 7-day rolling average
# HIGH_WIND         : windspeed > 80 kmph
# HEAVY_RAIN        : precipitation > 50 mm in one hour

try:
    df_existing = spark.read.format("delta").load(silver_path)
    from pyspark.sql.functions import date_sub, current_date

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
    print(f"First run or no baseline — anomaly detection skipped: {str(e)[:80]}")
    df_anomaly = df_clean.withColumn("is_anomaly", lit(False)).withColumn(
        "anomaly_reason", lit(None).cast("string")
    )


# COMMAND ----------

# CELL 6 - Append to Silver Delta Lake

df_anomaly.write.format("delta").mode("append").option("mergeSchema", "true").save(
    silver_path
)

total_silver = spark.read.format("delta").load(silver_path).count()
print(f"Silver layer total records after this run: {total_silver}")
display(df_anomaly.groupBy("city_name", "is_anomaly").count())


# COMMAND ----------

# CELL 7 - Return Status to ADF
import json

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
