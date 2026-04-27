"""
Capital Markets Trade Generator
================================
Generates realistic trade execution events and writes them as JSON files
to a target directory (for Auto Loader) or pushes to Azure Event Hubs.

Usage:
  File mode:    python trade_generator.py --mode file --output-path ./output/trades/
  EventHub:     python trade_generator.py --mode eventhub --connection-string "Endpoint=sb://..."

  Options:
    --batch-size      Number of trades per batch (default: 10)
    --interval        Seconds between batches (default: 5)
    --duration        Total seconds to run, 0=infinite (default: 0)
    --include-bad     Include ~5% bad records for DQ testing (default: True)
"""

import json
import os
import sys
import csv
import random
import uuid
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Load seed data
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
SEED_DIR = SCRIPT_DIR.parent / "seed"


def load_csv(filename):
    """Load a seed CSV into a list of dicts."""
    filepath = SEED_DIR / filename
    if not filepath.exists():
        print(f"ERROR: Seed file not found: {filepath}")
        sys.exit(1)
    with open(filepath, "r") as f:
        return list(csv.DictReader(f))


def load_instruments():
    return {r["ticker"]: r for r in load_csv("instruments.csv")}


def load_traders():
    return {r["trader_id"]: r for r in load_csv("traders.csv")}


def load_counterparties():
    return [r["counterparty_id"] for r in load_csv("counterparties.csv")]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ORDER_TYPES = ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
ORDER_TYPE_WEIGHTS = [0.50, 0.30, 0.12, 0.08]
SIDES = ["BUY", "SELL"]
QUANTITY_RANGES = [
    (10, 100, 0.30),       # small retail
    (100, 1000, 0.40),     # standard institutional
    (1000, 5000, 0.20),    # large block
    (5000, 50000, 0.10),   # very large block
]


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class TradeGenerator:
    def __init__(self, include_bad_records=True):
        self.instruments = load_instruments()
        self.traders = load_traders()
        self.counterparties = load_counterparties()
        self.tickers = list(self.instruments.keys())
        self.trader_ids = list(self.traders.keys())
        self.trade_counter = 0
        self.include_bad = include_bad_records

        # Random walk price state per ticker
        self.price_state = {
            t: float(self.instruments[t]["base_price"]) for t in self.tickers
        }

    # ---- FIX #1: Trade ID now includes full timestamp (to microsecond) ----
    # Old: T-20260426-000001  (counter resets on restart = duplicates)
    # New: T-20260426-143205-123456-0001  (timestamp + microseconds + counter)
    #      Unique across restarts because the timestamp portion changes.
    #      Counter still present to disambiguate trades within the same
    #      microsecond (possible when generating batches quickly).
    def _next_trade_id(self):
        self.trade_counter += 1
        now = datetime.now(timezone.utc)
        ts_date = now.strftime("%Y%m%d")
        ts_time = now.strftime("%H%M%S")
        ts_micro = f"{now.microsecond:06d}"
        return f"T-{ts_date}-{ts_time}-{ts_micro}-{self.trade_counter:04d}"

    def _get_quantity(self):
        r = random.random()
        cumulative = 0
        for low, high, weight in QUANTITY_RANGES:
            cumulative += weight
            if r <= cumulative:
                return random.randint(low, high)
        return random.randint(100, 1000)

    def _get_price(self, ticker):
        """Random walk price movement (~0.2% std dev per tick)."""
        last = self.price_state[ticker]
        change_pct = random.gauss(0, 0.002)
        new_price = round(last * (1 + change_pct), 2)
        new_price = max(new_price, 0.01)  # floor
        self.price_state[ticker] = new_price
        return new_price

    def _generate_bad_record(self):
        """Generate an intentionally bad record for DQ testing."""
        bad_type = random.choice([
            "null_ticker",
            "negative_price",
            "zero_qty",
            "missing_trader",
            "future_timestamp",
        ])

        trade = self._generate_clean_trade()

        if bad_type == "null_ticker":
            trade["ticker"] = None
        elif bad_type == "negative_price":
            trade["price"] = -abs(trade["price"])
        elif bad_type == "zero_qty":
            trade["quantity"] = 0
        elif bad_type == "missing_trader":
            trade["trader_id"] = None
            trade["desk"] = None
        elif bad_type == "future_timestamp":
            trade["timestamp"] = "2030-12-31T23:59:59.999Z"

        trade["_dq_flag"] = bad_type  # hidden marker for debugging
        return trade

    def _generate_clean_trade(self):
        ticker = random.choice(self.tickers)
        trader_id = random.choice(self.trader_ids)
        trader = self.traders[trader_id]
        instrument = self.instruments[ticker]

        return {
            "trade_id": self._next_trade_id(),
            "ticker": ticker,
            "side": random.choice(SIDES),
            "quantity": self._get_quantity(),
            "price": self._get_price(ticker),
            "order_type": random.choices(
                ORDER_TYPES, weights=ORDER_TYPE_WEIGHTS, k=1
            )[0],
            "trader_id": trader_id,
            "desk": trader["desk"],
            "counterparty_id": random.choice(self.counterparties),
            "exchange": instrument["exchange"],
            "currency": instrument["currency"],
            "asset_class": instrument["asset_class"],
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S."
            )
            + f"{random.randint(0, 999):03d}Z",
        }

    def generate_batch(self, batch_size=10):
        trades = []
        for _ in range(batch_size):
            if self.include_bad and random.random() < 0.05:
                trades.append(self._generate_bad_record())
            else:
                trades.append(self._generate_clean_trade())
        return trades


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def write_to_file(trades, output_path):
    """Write a batch as a single JSON-lines file to the output directory."""
    os.makedirs(output_path, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    unique = uuid.uuid4().hex[:8]
    filename = f"trades_{timestamp}_{unique}.json"
    filepath = os.path.join(output_path, filename)
    with open(filepath, "w") as f:
        for trade in trades:
            f.write(json.dumps(trade) + "\n")
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"Wrote {len(trades)} trades -> {filepath}"
    )


def write_to_eventhub(trades, producer):
    """Send a batch of trades to Azure Event Hubs."""
    from azure.eventhub import EventData

    event_batch = producer.create_batch()
    for trade in trades:
        event_batch.add(EventData(json.dumps(trade)))
    producer.send_batch(event_batch)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"Sent {len(trades)} trades -> Event Hub"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Capital Markets Trade Generator"
    )
    parser.add_argument(
        "--mode", choices=["file", "eventhub"], default="file"
    )
    parser.add_argument("--output-path", default="./output/trades/")
    parser.add_argument("--connection-string", default=None)
    parser.add_argument("--eventhub-name", default="trades")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument(
        "--duration", type=int, default=0, help="Seconds to run (0=forever)"
    )
    parser.add_argument("--no-bad-records", action="store_true")
    args = parser.parse_args()

    generator = TradeGenerator(
        include_bad_records=not args.no_bad_records
    )
    producer = None

    if args.mode == "eventhub":
        if not args.connection_string:
            print("ERROR: --connection-string required for eventhub mode")
            sys.exit(1)
        from azure.eventhub import EventHubProducerClient

        producer = EventHubProducerClient.from_connection_string(
            args.connection_string, eventhub_name=args.eventhub_name
        )

    print(
        f"Trade Generator | mode={args.mode} | "
        f"batch={args.batch_size} | interval={args.interval}s"
    )
    print("Press Ctrl+C to stop\n")

    start_time = time.time()
    total_trades = 0

    try:
        while True:
            batch = generator.generate_batch(args.batch_size)
            total_trades += len(batch)

            if args.mode == "file":
                write_to_file(batch, args.output_path)
            else:
                write_to_eventhub(batch, producer)

            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Total trades generated: {total_trades}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
