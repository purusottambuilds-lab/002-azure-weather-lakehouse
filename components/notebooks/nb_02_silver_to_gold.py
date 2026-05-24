# Databricks notebook source
# ================================================================
# nb_02_silver_to_gold
# Project_002: Real-Time Weather Data Lakehouse
# Purpose: Daily aggregation, 7-day rolling window analytics, weather severity scoring, Gold layer write
# Author: Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# cell-1: read parameters from ADF

dbutils.widgets.text("run_date", "")

run_date = dbutils.widgets.get("run_date")

if not run_date:
    from datetime import datetime

    run_date = datetime.now().strftime("%Y-%m-%d-%H")

print(f"Run date: {run_date}")

# COMMAND ----------

# cell-2: storage configuration

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    avg,
    max,
    min,
    sum,
    count,
    round as r,
    to_date,
    current_timestamp,
    lit,
    when,
    stddev,
    avg as window_avg,
    sum as window_sum,
)
import json

storage_account_name = "saweatherps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

silver_path = (
    f"abfss://silver@{storage_account_name}.dfs.core.windows.net/weather_cleaned"
)
gold_summary_path = (
    f"abfss://gold@{storage_account_name}.dfs.core.windows.net/weather_daily_summary"
)
gold_anomaly_path = (
    f"abfss://gold@{storage_account_name}.dfs.core.windows.net/weather_anomalies"
)

print("Storage configured")

# COMMAND ----------

# cell-3: read silver data and add date column

df_silver = spark.read.format("delta").load(silver_path)
df_silver = df_silver.withColumn("date", to_date(col("datetime_ts")))

print(f"Silver records: {df_silver.count()}")
print(f'cities: {[r[0] for r in df_silver.select("city_name").distinct().collect()]}')

# COMMAND ----------

# cell-4: daily aggregation per city


df_daily = df_silver.groupBy(
    "date", "city_name", "latitude", "longitude", "timezone"
).agg(
    r(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    r(max("temperature_celsius"), 2).alias("max_temp_celsius"),
    r(min("temperature_celsius"), 2).alias("min_temp_celsius"),
    r(sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
    r(avg("windspeed_kmph"), 2).alias("avg_wind_speed_kmph"),
    r(max("windspeed_kmph"), 2).alias("max_wind_speed_kmph"),
    r(avg("humidity_pct"), 1).alias("avg_humidity_pct"),
    r(avg("feels_like_celsius"), 2).alias("avg_feels_like_celsius"),
    r(stddev("temperature_celsius"), 2).alias("temp_stddev"),
    count(when(col("is_anomaly") == True, 1)).alias("anomaly_hours"),
    count("datetime_ts").alias("data_hours"),
)

print(f"Daily aggregation rows: {df_daily.count()}")
display(df_daily.orderBy("date", "city_name"))

# COMMAND ----------

# cell-5: 7-day Rolling Window Analytics calculated per city independently

window_7d = Window.partitionBy("city_name").orderBy("date").rowsBetween(-6, 0)

df_rolling = (
    df_daily.withColumn(
        "rolling_7d_avg_temp", r(window_avg("avg_temp_celsius").over(window_7d), 2)
    )
    .withColumn(
        "rolling_7d_total_rain",
        r(window_sum("total_precipitation_mm").over(window_7d), 2),
    )
    .withColumn(
        "rolling_7d_avg_wind",
        r(
            window_avg("avg_wind_speed_kmph").over(window_7d),
        ),
    )
)

print("7-day rolling window analytics added")

display(
    df_rolling.select(
        "date",
        "city_name",
        "avg_temp_celsius",
        "rolling_7d_avg_temp",
        "rolling_7d_total_rain",
    ).orderBy("city_name", "date")
)

# COMMAND ----------

# cell-6: Weather Severity Scoring

# SEVERE: avg_temp > 40 OR total_rain > 100 OR max_wind > 80
# MODERATE: avg_temp > 35 OR total_rain > 50 OR max_wind > 50 OR any anomalies
# NORMAL: everything else

df_gold = df_rolling.withColumn(
    "weather_severity",
    when(
        (col("avg_temp_celsius") > 40)
        | (col("total_precipitation_mm") > 100)
        | (col("max_wind_speed_kmph") > 80),
        lit("SEVERE"),
    )
    .when(
        (col("avg_temp_celsius") > 35)
        | (col("total_precipitation_mm") > 50)
        | (col("max_wind_speed_kmph") > 50)
        | (col("anomaly_hours") > 0),
        lit("MODERATE"),
    )
    .otherwise(lit("NORMAL")),
).withColumn("gold_updated_at", current_timestamp())

print("Severity Distribution:")
display(df_gold.groupBy("city_name", "weather_severity").count())

# COMMAND ----------

# cell-7: write Gold Summary (OVERWRITE)

df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    gold_summary_path
)

print(f"Gold summary written to: {gold_summary_path}")

# COMMAND ----------

# cell-8: extract anomalies to Gold anomalies table

df_anomalies = (
    df_silver.filter(col("is_anomaly") == True)
    .select(
        "date",
        "city_name",
        "datetime_ts",
        "temperature_celsius",
        "precipitation_mm",
        "windspeed_kmph",
        "anomaly_reason",
        "ingested_at",
    )
    .withColumn("gold_updated_at", current_timestamp())
)

df_anomalies.write.format("delta").mode("overwrite").save(gold_anomaly_path)

print(f"Anomaly records written: {df_anomalies.count()}")
display(df_anomalies.orderBy("datetime_ts", ascending=False).limit(10))

# COMMAND ----------

# cell-9: return status to ADF

exit_value = json.dumps(
    {
        "status": "SUCCESS",
        "gold_rows": df_gold.count(),
        "anomaly_rows": df_anomalies.count(),
        "run_date": run_date,
    }
)

dbutils.notebook.exit(exit_value)

# COMMAND ----------
