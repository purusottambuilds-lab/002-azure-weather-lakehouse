# Databricks notebook source
# ================================================================
# nb_03_gold_to_sql
# Project 002   : Real-Time Weather Data Lakehouse
# Purpose       : Load Gold Delta tables to Azure SQL Database via JDBC
# Folder        : dir_002_weather_lakehouse
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1: Storage and SQL Configuration

storage_account_name = "saweatherps01"
storage_account_key = "YOUR_STORAGE_ACCOUNT_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

sql_server = "sql-buildlab-de-ps01.database.windows.net"
sql_database = "db-weather"
sql_user = "sqladmin-buildlab-de-ps01"
sql_password = "YOUR_SQL_PASSWORD_HERE"

sql_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    f"user={sql_user};"
    f"password={sql_password};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30"
)

gold_summary_path = (
    f"abfss://gold@{storage_account_name}.dfs.core.windows.net/weather_daily_summary"
)
gold_anomaly_path = (
    f"abfss://gold@{storage_account_name}.dfs.core.windows.net/weather_anomalies"
)

print("Storage and SQL configuration set")


# COMMAND ----------

# CELL 2 - Write Gold Summary to Azure SQL
df_summary = spark.read.format("delta").load(gold_summary_path)
print(f"Gold summary records: {df_summary.count()}")

df_summary.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.weather_daily_summary"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.weather_daily_summary: written successfully")


# COMMAND ----------

# CELL 3 - Write Anomalies to Azure SQL

df_anomalies = spark.read.format("delta").load(gold_anomaly_path)
print(f"Anomaly records: {df_anomalies.count()}")

df_anomalies.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.weather_anomalies"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.weather_anomalies: written successfully")


# COMMAND ----------

# CELL 4 - Verify SQL Tables

df_verify = (
    spark.read.format("jdbc")
    .option("url", sql_url)
    .option("dbtable", "dbo.weather_daily_summary")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

print(f"SQL dbo.weather_daily_summary rows: {df_verify.count()}")
display(df_verify.orderBy("date", "city_name").limit(10))


# COMMAND ----------

# CELL 5 - Return Status to ADF

import json

exit_value = json.dumps(
    {
        "status": "SUCCESS",
        "sql_summary_rows": df_summary.count(),
        "sql_anomaly_rows": df_anomalies.count(),
    }
)
dbutils.notebook.exit(exit_value)


# COMMAND ----------
