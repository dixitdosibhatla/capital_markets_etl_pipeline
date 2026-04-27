# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Layer - Raw Ingestion
# MAGIC Ingests trade executions and market data via Auto Loader (streaming),
# MAGIC and reference data from Azure SQL DB (batch).

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ===========================================================================
# CONFIGURATION - Uses Databricks Secrets (scope: capital-markets)
# ===========================================================================
STORAGE_ACCOUNT = spark.conf.get("spark.secret.storage_account",
    default=dbutils.secrets.get(scope="capital-markets", key="storage-account"))
CONTAINER = "capital-markets"
ADLS_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

TRADES_LANDING_PATH = f"{ADLS_BASE}/landing/trades/"
MARKET_DATA_LANDING_PATH = f"{ADLS_BASE}/landing/market_data/"

# JDBC config via Databricks Secrets
_JDBC_HOST = dbutils.secrets.get(scope="capital-markets", key="sql-host")
_JDBC_DB = dbutils.secrets.get(scope="capital-markets", key="sql-database")
JDBC_URL = f"jdbc:sqlserver://{_JDBC_HOST}:1433;database={_JDBC_DB};encrypt=true;trustServerCertificate=false;"
JDBC_USER = dbutils.secrets.get(scope="capital-markets", key="sql-username")
JDBC_PASS = dbutils.secrets.get(scope="capital-markets", key="sql-password")

# ===========================================================================
# TRADE RAW SCHEMA
# ===========================================================================
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

TRADE_RAW_SCHEMA = StructType([
    StructField("trade_id", StringType(), False),
    StructField("ticker", StringType(), True),
    StructField("side", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_type", StringType(), True),
    StructField("trader_id", StringType(), True),
    StructField("desk", StringType(), True),
    StructField("counterparty_id", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("_dq_flag", StringType(), True),
])

MARKET_DATA_RAW_SCHEMA = StructType([
    StructField("tick_id", StringType(), False),
    StructField("ticker", StringType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("spread", DoubleType(), True),
    StructField("tick_volume", IntegerType(), True),
    StructField("cumulative_volume", IntegerType(), True),
    StructField("high_52w", DoubleType(), True),
    StructField("low_52w", DoubleType(), True),
    StructField("exchange", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("timestamp", StringType(), True),
])


# ===========================================================================
# BRONZE: RAW TRADES (Streaming via Auto Loader)
# ===========================================================================
@dlt.table(
    name="bronze.raw_trades",
    comment="Raw trade execution events ingested via Auto Loader from ADLS",
    table_properties={"quality": "bronze"},
)
def raw_trades():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{ADLS_BASE}/schema/trades/")
        .option("cloudFiles.inferColumnTypes", "true")
        .schema(TRADE_RAW_SCHEMA)
        .load(TRADES_LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ===========================================================================
# BRONZE: RAW MARKET DATA (Streaming via Auto Loader)
# ===========================================================================
@dlt.table(
    name="bronze.raw_market_data",
    comment="Raw market data ticks ingested via Auto Loader from ADLS",
    table_properties={"quality": "bronze"},
)
def raw_market_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{ADLS_BASE}/schema/market_data/")
        .option("cloudFiles.inferColumnTypes", "true")
        .schema(MARKET_DATA_RAW_SCHEMA)
        .load(MARKET_DATA_LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ===========================================================================
# BRONZE: REFERENCE DATA (Batch from Azure SQL DB)
# ===========================================================================
@dlt.table(
    name="bronze.raw_instruments",
    comment="Instrument master data loaded from Azure SQL DB",
    table_properties={"quality": "bronze"},
)
def raw_instruments():
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "source.instruments")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze.raw_traders",
    comment="Trader and desk data loaded from Azure SQL DB",
    table_properties={"quality": "bronze"},
)
def raw_traders():
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "source.traders")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze.raw_counterparties",
    comment="Counterparty data loaded from Azure SQL DB",
    table_properties={"quality": "bronze"},
)
def raw_counterparties():
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "source.counterparties")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
    )
