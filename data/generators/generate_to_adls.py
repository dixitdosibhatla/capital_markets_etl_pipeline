"""
Generate trades and market data directly to ADLS Gen2.
Usage:
    python generate_to_adls.py --duration 60 --trade-interval 5 --market-interval 3

Requires: pip install azure-storage-blob python-dotenv
Create a .env file in the same directory with: AZURE_STORAGE_KEY=your-key-here
"""
from dotenv import load_dotenv
load_dotenv()

import json, os, time, uuid, argparse, threading
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

# ---- CONFIGURATION ----
STORAGE_ACCOUNT = "stcapmarketsetl"
CONTAINER = "capital-markets"
ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_KEY")

# Import your existing generators
from trade_generator import TradeGenerator
from market_data_generator import MarketDataGenerator


def get_blob_service():
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def upload_json_batch(container_client, folder, prefix, records):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"{folder}/{prefix}_{ts}_{uuid.uuid4().hex[:8]}.json"
    content = "\n".join(json.dumps(r) for r in records)
    container_client.upload_blob(name=blob_name, data=content, overwrite=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Uploaded {len(records)} records -> {blob_name}")


def run_trade_generator(container_client, batch_size, interval, stop_event):
    gen = TradeGenerator(include_bad_records=True)
    total = 0
    while not stop_event.is_set():
        batch = gen.generate_batch(batch_size)
        upload_json_batch(container_client, "landing/trades", "trades", batch)
        total += len(batch)
        stop_event.wait(interval)
    print(f"Trade generator stopped. Total trades: {total}")


def run_market_generator(container_client, batch_size, interval, stop_event):
    gen = MarketDataGenerator()
    total = 0
    while not stop_event.is_set():
        batch = gen.generate_batch(batch_size)
        upload_json_batch(container_client, "landing/market_data", "market", batch)
        total += len(batch)
        stop_event.wait(interval)
    print(f"Market data generator stopped. Total ticks: {total}")


def main():
    parser = argparse.ArgumentParser(description="Generate data directly to ADLS")
    parser.add_argument("--duration", type=int, default=60, help="Seconds to run (default 60)")
    parser.add_argument("--trade-batch", type=int, default=50, help="Trades per batch")
    parser.add_argument("--trade-interval", type=float, default=5.0, help="Seconds between trade batches")
    parser.add_argument("--market-batch", type=int, default=50, help="Market ticks per batch")
    parser.add_argument("--market-interval", type=float, default=3.0, help="Seconds between market batches")
    args = parser.parse_args()

    if not ACCOUNT_KEY:
        print("ERROR: AZURE_STORAGE_KEY not found.")
        print("Create a .env file in this directory with:")
        print("  AZURE_STORAGE_KEY=your-key-here")
        return

    blob_service = get_blob_service()
    container_client = blob_service.get_container_client(CONTAINER)

    print(f"Generating to ADLS: {STORAGE_ACCOUNT}/{CONTAINER}")
    print(f"  Trades: batch={args.trade_batch}, interval={args.trade_interval}s")
    print(f"  Market: batch={args.market_batch}, interval={args.market_interval}s")
    print(f"  Duration: {args.duration}s")
    print("Press Ctrl+C to stop early\n")

    stop_event = threading.Event()

    trade_thread = threading.Thread(
        target=run_trade_generator,
        args=(container_client, args.trade_batch, args.trade_interval, stop_event)
    )
    market_thread = threading.Thread(
        target=run_market_generator,
        args=(container_client, args.market_batch, args.market_interval, stop_event)
    )

    trade_thread.start()
    market_thread.start()

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\nStopping early...")
    finally:
        stop_event.set()
        trade_thread.join()
        market_thread.join()
        print("\nDone! Now go to Databricks and run the pipeline.")


if __name__ == "__main__":
    main()