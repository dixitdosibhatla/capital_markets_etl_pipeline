# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Gold Layer - Business Analytics
# MAGIC Produces P&L by desk, risk exposure by sector, trade anomaly detection,
# MAGIC counterparty exposure, hourly volume aggregation, and mark-to-market P&L.
# MAGIC
# MAGIC Power BI connects directly to these Gold tables via SQL Warehouse.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ===========================================================================
# CONFIGURATION
# Fully qualified Silver table names — update catalog name if yours differs
# ===========================================================================
CATALOG = "capital_markets_etl"
SILVER = f"{CATALOG}.silver"

# ===========================================================================
# GOLD: DESK P&L
# Real-time P&L per trading desk
# ===========================================================================
@dlt.table(
    name="gold.desk_pnl",
    comment="P&L summary per trading desk - connected to Power BI",
    table_properties={"quality": "gold"},
)
def desk_pnl():
    trades = spark.read.table(f"{SILVER}.enriched_trades")

    return (trades
        .withColumn(
            "signed_notional",
            F.when(F.col("side") == "SELL", F.col("notional_usd"))
             .otherwise(-F.col("notional_usd"))
        )
        .groupBy("desk", "desk_full_name")
        .agg(
            F.sum("signed_notional").alias("net_pnl_usd"),
            F.sum(F.abs(F.col("notional_usd"))).alias("gross_volume_usd"),
            F.count("trade_id").alias("trade_count"),
            F.countDistinct("ticker").alias("unique_tickers"),
            F.countDistinct("trader_id").alias("active_traders"),
            F.avg("notional_usd").alias("avg_trade_size_usd"),
            F.max("event_time").alias("last_trade_time"),
            F.sum(F.when(F.col("side") == "BUY", 1).otherwise(0)).alias("buy_count"),
            F.sum(F.when(F.col("side") == "SELL", 1).otherwise(0)).alias("sell_count"),
        )
        .withColumn("buy_sell_ratio",
            F.round(F.col("buy_count") / F.greatest(F.col("sell_count"), F.lit(1)), 2)
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


# ===========================================================================
# GOLD: RISK EXPOSURE BY SECTOR
# Net and gross exposure grouped by sector and asset class
# ===========================================================================
@dlt.table(
    name="gold.risk_exposure_by_sector",
    comment="Risk exposure analysis by sector and asset class",
    table_properties={"quality": "gold"},
)
def risk_exposure_by_sector():
    trades = spark.read.table(f"{SILVER}.enriched_trades")

    return (trades
        .groupBy("sector", "asset_class")
        .agg(
            F.sum(
                F.when(F.col("side") == "BUY", F.col("notional_usd")).otherwise(0)
            ).alias("long_notional_usd"),
            F.sum(
                F.when(F.col("side") == "SELL", F.col("notional_usd")).otherwise(0)
            ).alias("short_notional_usd"),
            F.count("trade_id").alias("trade_count"),
            F.countDistinct("ticker").alias("unique_tickers"),
            F.countDistinct("desk").alias("active_desks"),
        )
        .withColumn("net_exposure_usd",
            F.col("long_notional_usd") - F.col("short_notional_usd")
        )
        .withColumn("gross_exposure_usd",
            F.col("long_notional_usd") + F.col("short_notional_usd")
        )
        .withColumn("long_short_ratio",
            F.round(
                F.when(F.col("short_notional_usd") > 0,
                    F.col("long_notional_usd") / F.col("short_notional_usd")
                ).otherwise(F.lit(None)),
                4
            )
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


# ===========================================================================
# GOLD: COUNTERPARTY EXPOSURE
# Concentration risk by counterparty
# ===========================================================================
@dlt.table(
    name="gold.counterparty_exposure",
    comment="Counterparty concentration risk - notional exposure, trade count, risk rating",
    table_properties={"quality": "gold"},
)
def counterparty_exposure():
    trades = spark.read.table(f"{SILVER}.enriched_trades")

    return (trades
        .groupBy("counterparty_id", "counterparty_name", "risk_rating",
                 "counterparty_type", "cp_country")
        .agg(
            F.sum(
                F.when(F.col("side") == "BUY", F.col("notional_usd")).otherwise(0)
            ).alias("long_notional_usd"),
            F.sum(
                F.when(F.col("side") == "SELL", F.col("notional_usd")).otherwise(0)
            ).alias("short_notional_usd"),
            F.count("trade_id").alias("trade_count"),
            F.countDistinct("ticker").alias("unique_tickers"),
            F.countDistinct("desk").alias("active_desks"),
            F.avg("notional_usd").alias("avg_trade_size_usd"),
            F.max("event_time").alias("last_trade_time"),
        )
        .withColumn("net_exposure_usd",
            F.col("long_notional_usd") - F.col("short_notional_usd")
        )
        .withColumn("gross_exposure_usd",
            F.col("long_notional_usd") + F.col("short_notional_usd")
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


# ===========================================================================
# GOLD: TRADE ANOMALIES
# Flags trades with unusual size, price deviation, or risk limit breach
# ===========================================================================
@dlt.table(
    name="gold.trade_anomalies",
    comment="Detected trade anomalies - quantity spikes, price deviations, risk breaches",
    table_properties={"quality": "gold"},
)
def trade_anomalies():
    trades = spark.read.table(f"{SILVER}.enriched_trades")

    ticker_window = Window.partitionBy("ticker")

    flagged = (trades
        .withColumn("avg_qty", F.avg("quantity").over(ticker_window))
        .withColumn("stddev_qty", F.stddev("quantity").over(ticker_window))
        .withColumn("avg_price", F.avg("price").over(ticker_window))
        # Quantity anomaly: > 3 std deviations above mean
        .withColumn("is_qty_anomaly",
            F.col("quantity") > (
                F.col("avg_qty") + 3 * F.coalesce(F.col("stddev_qty"), F.lit(0))
            )
        )
        # Price deviation: > 2% from ticker average
        .withColumn("price_deviation_pct",
            F.round(
                F.abs(F.col("price") - F.col("avg_price")) / F.col("avg_price") * 100, 2
            )
        )
        .withColumn("is_price_anomaly", F.col("price_deviation_pct") > 2.0)
        # Risk limit breach
        .withColumn("is_risk_breach",
            F.col("notional_usd") > F.col("risk_limit_usd")
        )
        # Combined
        .withColumn("is_anomaly",
            F.col("is_qty_anomaly") | F.col("is_price_anomaly") | F.col("is_risk_breach")
        )
    )

    return (flagged
        .filter(F.col("is_anomaly") == True)
        .select(
            "trade_id", "ticker", "company_name", "side",
            "quantity", "price", "notional_usd",
            "trader_id", "trader_name", "desk", "desk_full_name", "sector",
            "counterparty_id", "counterparty_name", "risk_rating", "cp_country",
            "is_qty_anomaly", "is_price_anomaly", "is_risk_breach",
            "price_deviation_pct",
            "avg_qty", "avg_price", "risk_limit_usd",
            "event_time", "trade_date",
        )
        .withColumn("detected_at", F.current_timestamp())
    )


# ===========================================================================
# GOLD: HOURLY TRADE VOLUME
# Trade volume aggregated by hour for time-series dashboards
# ===========================================================================
@dlt.table(
    name="gold.hourly_trade_volume",
    comment="Hourly trade volume and notional by desk and sector",
    table_properties={"quality": "gold"},
)
def hourly_trade_volume():
    trades = spark.read.table(f"{SILVER}.enriched_trades")

    return (trades
        .withColumn("trade_hour",
            F.date_trunc("hour", F.col("event_time"))
        )
        .groupBy("trade_hour", "desk", "sector")
        .agg(
            F.count("trade_id").alias("trade_count"),
            F.sum("notional_usd").alias("total_notional_usd"),
            F.avg("price").alias("avg_price"),
            F.sum("quantity").alias("total_quantity"),
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


# ===========================================================================
# GOLD: MARK-TO-MARKET P&L
# Joins positions with latest market prices for unrealized P&L
# ===========================================================================
@dlt.table(
    name="gold.mark_to_market_pnl",
    comment="Unrealized P&L per ticker per desk using latest market prices",
    table_properties={"quality": "gold"},
)
def mark_to_market_pnl():
    positions = spark.read.table(f"{SILVER}.positions")
    prices = spark.read.table(f"{SILVER}.latest_market_prices")

    prices_slim = prices.select(
        F.col("ticker").alias("mkt_ticker"),
        "last_price",
        "bid",
        "ask",
        "mid_price",
        "spread_bps",
        F.col("event_time").alias("price_as_of"),
    )

    return (positions
        .join(
            prices_slim,
            positions.ticker == prices_slim.mkt_ticker,
            "left",
        )
        .drop("mkt_ticker")
        # Current market value = net shares * latest price
        .withColumn("market_value_usd",
            F.round(F.col("net_quantity") * F.col("last_price"), 2)
        )
        # Unrealized P&L = market value - cost basis (net_notional from positions)
        .withColumn("unrealized_pnl_usd",
            F.round(F.col("market_value_usd") - F.col("net_notional_usd"), 2)
        )
        # P&L as a percentage of cost basis
        .withColumn("pnl_pct",
            F.round(
                F.when(F.abs(F.col("net_notional_usd")) > 0,
                    (F.col("unrealized_pnl_usd") / F.abs(F.col("net_notional_usd"))) * 100
                ).otherwise(F.lit(0)),
                2
            )
        )
        .select(
            "ticker",
            "desk",
            "sector",
            "asset_class",
            "position_side",
            "net_quantity",
            "avg_price",
            "last_price",
            "mid_price",
            "bid",
            "ask",
            "spread_bps",
            "net_notional_usd",
            "market_value_usd",
            "unrealized_pnl_usd",
            "pnl_pct",
            "gross_notional_usd",
            "trade_count",
            "last_trade_time",
            "price_as_of",
        )
        .withColumn("calculated_at", F.current_timestamp())
    )
