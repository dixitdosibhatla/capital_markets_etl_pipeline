# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver Layer - Enrichment, Validation & SCD
# MAGIC Cleans, deduplicates, and enriches raw trades with reference data.
# MAGIC Implements SCD Type 2 for traders and counterparties via `dlt.apply_changes()`.
# MAGIC Applies DLT expectations for data quality enforcement.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ===========================================================================
# SCD TYPE 2: TRADERS
# ===========================================================================
@dlt.view(name="v_raw_traders_stream")
def v_raw_traders_stream():
    return spark.readStream.option("skipChangeCommits", "true").table("LIVE.bronze.raw_traders")

dlt.create_streaming_table(
    name="silver.scd2_traders",
    comment="SCD Type 2 table for traders - tracks desk transfers, risk limit changes, manager changes",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver.scd2_traders",
    source="v_raw_traders_stream",
    keys=["trader_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
)


# ===========================================================================
# SCD TYPE 2: COUNTERPARTIES
# ===========================================================================
@dlt.view(name="v_raw_counterparties_stream")
def v_raw_counterparties_stream():
    return spark.readStream.option("skipChangeCommits", "true").table("LIVE.bronze.raw_counterparties")

dlt.create_streaming_table(
    name="silver.scd2_counterparties",
    comment="SCD Type 2 table for counterparties - tracks risk rating and status changes",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver.scd2_counterparties",
    source="v_raw_counterparties_stream",
    keys=["counterparty_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
)


# ===========================================================================
# SILVER: ENRICHED TRADES
# ===========================================================================
@dlt.table(
    name="silver.enriched_trades",
    comment="Trades enriched with instrument, trader (SCD2), and counterparty (SCD2) details, validated and deduplicated",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_trade_id", "trade_id IS NOT NULL")
@dlt.expect_or_drop("valid_ticker", "ticker IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect("valid_side", "side IN ('BUY', 'SELL')")
@dlt.expect("valid_trader", "trader_id IS NOT NULL")
@dlt.expect("valid_counterparty", "counterparty_id IS NOT NULL")
@dlt.expect("valid_timestamp", "event_time IS NOT NULL AND event_time < '2028-01-01'")
def enriched_trades():
    trades = dlt.read_stream("bronze.raw_trades")
    instruments = dlt.read("bronze.raw_instruments")
    traders = dlt.read("silver.scd2_traders")
    counterparties = dlt.read("silver.scd2_counterparties")

    trades_parsed = trades.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )

    trades_deduped = trades_parsed.dropDuplicates(["trade_id"])

    instruments_slim = instruments.select(
        F.col("ticker").alias("inst_ticker"),
        "company_name",
        "sector",
    )
    trades_with_inst = (trades_deduped
        .join(instruments_slim, trades_deduped.ticker == instruments_slim.inst_ticker, "left")
        .drop("inst_ticker")
    )

    traders_slim = (traders
        .filter("__end_at IS NULL")
        .select(
            F.col("trader_id").alias("tr_trader_id"),
            "trader_name",
            "desk_full_name",
            "risk_limit_usd",
            "location",
        )
    )
    trades_with_traders = (trades_with_inst
        .join(traders_slim, trades_with_inst.trader_id == traders_slim.tr_trader_id, "left")
        .drop("tr_trader_id")
    )

    counterparties_slim = (counterparties
        .filter("__end_at IS NULL")
        .select(
            F.col("counterparty_id").alias("cp_counterparty_id"),
            "counterparty_name",
            "risk_rating",
            "counterparty_type",
            F.col("country").alias("cp_country"),
        )
    )
    trades_enriched = (trades_with_traders
        .join(counterparties_slim,
              trades_with_traders.counterparty_id == counterparties_slim.cp_counterparty_id, "left")
        .drop("cp_counterparty_id")
    )

    result = (trades_enriched
        .withColumn("notional_usd", F.round(F.col("price") * F.col("quantity"), 2))
        .withColumn("trade_date", F.to_date(F.col("event_time")))
        .withColumn("trade_hour", F.hour(F.col("event_time")))
        .withColumn("processed_at", F.current_timestamp())
    )

    return result


# ===========================================================================
# SILVER: ENRICHED MARKET DATA
# ===========================================================================
@dlt.table(
    name="silver.enriched_market_data",
    comment="Validated market data ticks with parsed timestamps",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_tick_id", "tick_id IS NOT NULL")
@dlt.expect_or_drop("valid_md_ticker", "ticker IS NOT NULL")
@dlt.expect_or_drop("valid_bid", "bid > 0")
@dlt.expect_or_drop("valid_ask", "ask > 0")
@dlt.expect("bid_less_than_ask", "bid <= ask")
def enriched_market_data():
    market_data = dlt.read_stream("bronze.raw_market_data")

    return (market_data
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        .withColumn("mid_price", F.round((F.col("bid") + F.col("ask")) / 2, 2))
        .withColumn("spread_bps",
            F.round(F.col("spread") / ((F.col("bid") + F.col("ask")) / 2) * 10000, 2)
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ===========================================================================
# SILVER: LATEST MARKET PRICES
# ===========================================================================
@dlt.table(
    name="silver.latest_market_prices",
    comment="Most recent market price per ticker for mark-to-market calculations",
    table_properties={"quality": "silver"},
)
def latest_market_prices():
    market_data = dlt.read("silver.enriched_market_data")

    window = Window.partitionBy("ticker").orderBy(F.col("event_time").desc())

    return (market_data
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .select(
            "ticker", "bid", "ask", "last_price", "mid_price",
            "spread", "spread_bps", "tick_volume", "cumulative_volume",
            "event_time"
        )
    )


# ===========================================================================
# SILVER: POSITION SNAPSHOTS
# ===========================================================================
@dlt.table(
    name="silver.positions",
    comment="Net position per ticker per desk - quantity and notional",
    table_properties={"quality": "silver"},
)
def positions():
    trades = dlt.read("silver.enriched_trades")

    return (trades
        .withColumn(
            "signed_quantity",
            F.when(F.col("side") == "BUY", F.col("quantity"))
             .otherwise(-F.col("quantity"))
        )
        .withColumn(
            "signed_notional",
            F.when(F.col("side") == "BUY", F.col("notional_usd"))
             .otherwise(-F.col("notional_usd"))
        )
        .groupBy("ticker", "desk", "sector", "asset_class")
        .agg(
            F.sum("signed_quantity").alias("net_quantity"),
            F.sum("signed_notional").alias("net_notional_usd"),
            F.sum(F.abs(F.col("notional_usd"))).alias("gross_notional_usd"),
            F.count("trade_id").alias("trade_count"),
            F.avg("price").alias("avg_price"),
            F.max("event_time").alias("last_trade_time"),
        )
        .withColumn("position_side",
            F.when(F.col("net_quantity") > 0, "LONG")
             .when(F.col("net_quantity") < 0, "SHORT")
             .otherwise("FLAT")
        )
        .withColumn("calculated_at", F.current_timestamp())
    )