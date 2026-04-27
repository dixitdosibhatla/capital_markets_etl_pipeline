# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup - Capital Markets ETL
# MAGIC Run this notebook ONCE to create the catalog and schemas.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks workspace with Unity Catalog enabled (Premium tier)
# MAGIC - Access Connector `ac-capmarkets-unity` created in Azure
# MAGIC - Storage Blob Data Contributor role assigned to Access Connector on ADLS
# MAGIC - Storage Credential `sc-capmarkets-adls` created in Databricks
# MAGIC - External Location `el-capmarkets` created and tested in Databricks
# MAGIC - Databricks Secrets scope `capital-markets` with key `storage-account`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Catalog with External Storage

# COMMAND ----------

STORAGE_ACCOUNT = dbutils.secrets.get(scope="capital-markets", key="storage-account")
CONTAINER = "capital-markets"
CATALOG_LOCATION = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/unity-catalog"

spark.sql(f"CREATE CATALOG IF NOT EXISTS capital_markets_etl MANAGED LOCATION '{CATALOG_LOCATION}'")
spark.sql("USE CATALOG capital_markets_etl")
print(f"Catalog 'capital_markets_etl' created with managed location: {CATALOG_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas (one per medallion layer)

# COMMAND ----------

schemas = ["bronze", "silver", "gold"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"  Schema '{schema}' created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Structure

# COMMAND ----------

display(spark.sql("SHOW SCHEMAS IN capital_markets_etl"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify JDBC Connection to Azure SQL DB

# COMMAND ----------

JDBC_HOST = dbutils.secrets.get(scope="capital-markets", key="sql-host")
JDBC_DB = dbutils.secrets.get(scope="capital-markets", key="sql-database")
JDBC_USER = dbutils.secrets.get(scope="capital-markets", key="sql-username")
JDBC_PASS = dbutils.secrets.get(scope="capital-markets", key="sql-password")
JDBC_URL = f"jdbc:sqlserver://{JDBC_HOST}:1433;database={JDBC_DB};encrypt=true;trustServerCertificate=false;"

for table in ["source.instruments", "source.traders", "source.counterparties"]:
    count = spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASS) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load().count()
    print(f"  {table}: {count} rows")

print("\nAll source tables verified!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC **Unity Catalog structure:**
# MAGIC ```
# MAGIC capital_markets_etl (Catalog)
# MAGIC ├── bronze    → raw_trades, raw_market_data, raw_instruments, raw_traders, raw_counterparties
# MAGIC ├── silver    → enriched_trades, enriched_market_data, scd2_traders, scd2_counterparties, latest_market_prices, positions
# MAGIC └── gold      → desk_pnl, risk_exposure_by_sector, counterparty_exposure, trade_anomalies, hourly_trade_volume, mark_to_market_pnl
# MAGIC ```
# MAGIC
# MAGIC **Security:**
# MAGIC - All credentials stored in Databricks Secrets (scope: `capital-markets`)
# MAGIC - Delta tables stored in external ADLS Gen2 via External Location
# MAGIC - Access Connector uses managed identity (no keys)
# MAGIC
# MAGIC **Next step:** Create and run the DLT pipeline (Bronze → Silver → Gold)