# Databricks notebook source
# ================================================================
# nb_02_silver_to_gold
# Project_002: Real-Time Weather Data Lakehouse
# Purpose: Load Gold Delta tables to Azure SQL Database via JDBC
# Author: Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# cell-1: storage and sql configuration

storage_account_name = "saweatherps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

# Azure SQL JDBC connection string

sql_server = "sql-buildlab-de-ps01.database.windows.net"
sql_database = "db-weather"
sql_user = "sqladmin-buildlab-de-ps01"
sql_password = "YOUR_SQL_PASSWORD_HERE"

sql_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"databaseName={sql_database};"
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

# cell-2: test connection before writing

try:
    df_test = (
        spark.read.format("jdbc")
        .option("url", sql_url)
        .option("query", "SELECT 1 AS test")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    print("SQL connection: SUCCESS")

except Exception as e:
    raise Exception(f"SQL connection FAILED: {e}")

# COMMAND ----------

# cell-3: write Gold summary to Azure SQL

df_summary = spark.read.format("delta").load(gold_summary_path)
print(f"Gold summary records: {df_summary.count()}")

df_summary.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.weather_daily_summary"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.weather_anomalies: written successfully")

# COMMAND ----------

# cell-4: write anomalies to Azure SQL

df_anomalies = spark.read.format("delta").load(gold_anomaly_path)
print(f"Anomaly records: {df_anomalies.count()}")

df_anomalies.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.weather_anomalies"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.weather_anomalies: written successfully")

# COMMAND ----------

# cell-5: verify SQL tables

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

# cell-6: return status to ADF

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
