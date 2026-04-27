"""
Capital Markets Market Data Generator
======================================
Generates continuous price tick events simulating a market data feed.
Each tick contains bid/ask/last price, volume, and spread for a ticker.

Usage:
  File mode:    python market_data_generator.py --mode file --output-path ./output/market_data/
  EventHub:     python market_data_generator.py --mode eventhub --connection-string "Endpoint=sb://..."

  Options:
    --batch-size      Number of ticks per batch (default: 20)
    --interval        Seconds between batches (default: 3)
    --duration        Total seconds to run, 0=infinite (default: 0)
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


SCRIPT_DIR = Path(__file__).resolve().parent
SEED_DIR = SCRIPT_DIR.parent / "seed"


def load_instruments():
    """Load instrument seed data."""
    filepath = SEED_DIR / "instruments.csv"
    if not filepath.exists():
        print(f"ERROR: Seed file not found: {filepath}")
        sys.exit(1)
    with open(filepath, "r") as f:
        return {r["ticker"]: r for r in csv.DictReader(f)}


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class MarketDataGenerator:
    def __init__(self):
        self.instruments = load_instruments()
        self.tickers = list(self.instruments.keys())
        self.tick_counter = 0

        # Initialize price state with random walk
        self.price_state = {}
        for ticker, info in self.instruments.items():
            base = float(info["base_price"])
            self.price_state[ticker] = {
                "last": base,
                "bid": round(base * 0.9998, 2),
                "ask": round(base * 1.0002, 2),
                "volume_today": random.randint(100000, 5000000),
                "high_52w": round(base * random.uniform(1.10, 1.50), 2),
                "low_52w": round(base * random.uniform(0.50, 0.90), 2),
            }

    # ---- FIX: tick_id now includes full timestamp (matches trade_generator) ----
    # Old: MK-00000001  (counter resets on restart)
    # New: MK-20260426-143205-123456-0001  (timestamp + microseconds + counter)
    def _next_tick_id(self):
        self.tick_counter += 1
        now = datetime.now(timezone.utc)
        ts_date = now.strftime("%Y%m%d")
        ts_time = now.strftime("%H%M%S")
        ts_micro = f"{now.microsecond:06d}"
        return f"MK-{ts_date}-{ts_time}-{ts_micro}-{self.tick_counter:04d}"

    def _update_price(self, ticker):
        """Random walk price update with realistic spread."""
        state = self.price_state[ticker]
        change_pct = random.gauss(0, 0.001)  # tighter than trade gen
        new_last = round(state["last"] * (1 + change_pct), 2)
        new_last = max(new_last, 0.01)

        # Spread varies by asset class
        asset_class = self.instruments[ticker]["asset_class"]
        if asset_class == "ETF":
            spread_bps = random.uniform(1, 5)
        else:
            spread_bps = random.uniform(2, 15)

        spread = new_last * (spread_bps / 10000)
        new_bid = round(new_last - spread / 2, 2)
        new_ask = round(new_last + spread / 2, 2)

        tick_volume = random.randint(100, 10000)
        state["last"] = new_last
        state["bid"] = new_bid
        state["ask"] = new_ask
        state["volume_today"] += tick_volume

        return state, tick_volume

    def generate_tick(self, ticker=None):
        """Generate a single market data tick."""
        if ticker is None:
            ticker = random.choice(self.tickers)

        state, tick_volume = self._update_price(ticker)
        instrument = self.instruments[ticker]

        return {
            "tick_id": self._next_tick_id(),
            "ticker": ticker,
            "bid": state["bid"],
            "ask": state["ask"],
            "last_price": state["last"],
            "spread": round(state["ask"] - state["bid"], 4),
            "tick_volume": tick_volume,
            "cumulative_volume": state["volume_today"],
            "high_52w": state["high_52w"],
            "low_52w": state["low_52w"],
            "exchange": instrument["exchange"],
            "asset_class": instrument["asset_class"],
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S."
            )
            + f"{random.randint(0, 999):03d}Z",
        }

    def generate_batch(self, batch_size=20):
        """Generate ticks for multiple random tickers."""
        return [self.generate_tick() for _ in range(batch_size)]


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def write_to_file(ticks, output_path):
    """Write a batch as a single JSON-lines file to the output directory."""
    os.makedirs(output_path, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    unique = uuid.uuid4().hex[:8]
    filename = f"market_{timestamp}_{unique}.json"
    filepath = os.path.join(output_path, filename)
    with open(filepath, "w") as f:
        for tick in ticks:
            f.write(json.dumps(tick) + "\n")
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"Wrote {len(ticks)} ticks -> {filepath}"
    )


def write_to_eventhub(ticks, producer):
    """Send a batch of ticks to Azure Event Hubs."""
    from azure.eventhub import EventData

    event_batch = producer.create_batch()
    for tick in ticks:
        event_batch.add(EventData(json.dumps(tick)))
    producer.send_batch(event_batch)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"Sent {len(ticks)} ticks -> Event Hub"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Capital Markets Market Data Generator"
    )
    parser.add_argument(
        "--mode", choices=["file", "eventhub"], default="file"
    )
    parser.add_argument("--output-path", default="./output/market_data/")
    parser.add_argument("--connection-string", default=None)
    parser.add_argument("--eventhub-name", default="market-data")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--interval", type=float, default=3.0)
    parser.add_argument(
        "--duration", type=int, default=0, help="Seconds to run (0=forever)"
    )
    args = parser.parse_args()

    generator = MarketDataGenerator()
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
        f"Market Data Generator | mode={args.mode} | "
        f"batch={args.batch_size} | interval={args.interval}s"
    )
    print("Press Ctrl+C to stop\n")

    start_time = time.time()
    total = 0

    try:
        while True:
            batch = generator.generate_batch(args.batch_size)
            total += len(batch)
            if args.mode == "file":
                write_to_file(batch, args.output_path)
            else:
                write_to_eventhub(batch, producer)
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Total ticks generated: {total}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
