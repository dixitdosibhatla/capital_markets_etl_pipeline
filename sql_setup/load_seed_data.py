"""
Load Seed Data into Azure SQL DB
==================================
Reads seed CSVs and inserts them into the source schema tables.

Usage:
    python load_seed_data.py \
        --server your-server.database.windows.net \
        --database capital_markets_db \
        --username your_user \
        --password your_pass

Requires: pip install pyodbc
"""

import csv, argparse, sys
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("ERROR: pip install pyodbc"); sys.exit(1)

SEED_DIR = Path(__file__).resolve().parent.parent / "data" / "seed"


def get_connection(args):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={args.server};"
        f"DATABASE={args.database};"
        f"UID={args.username};"
        f"PWD={args.password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)


def load_instruments(cursor):
    filepath = SEED_DIR / "instruments.csv"
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cursor.execute(
                "INSERT INTO source.instruments (ticker, company_name, sector, asset_class, currency, exchange, base_price) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                r["ticker"], r["company_name"], r["sector"], r["asset_class"],
                r["currency"], r["exchange"], float(r["base_price"])
            )
    print(f"  Loaded instruments.csv")


def load_traders(cursor):
    filepath = SEED_DIR / "traders.csv"
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cursor.execute(
                "INSERT INTO source.traders (trader_id, trader_name, desk, desk_full_name, risk_limit_usd, manager, location) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                r["trader_id"], r["trader_name"], r["desk"], r["desk_full_name"],
                float(r["risk_limit_usd"]), r["manager"] or None, r["location"]
            )
    print(f"  Loaded traders.csv")


def load_counterparties(cursor):
    filepath = SEED_DIR / "counterparties.csv"
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cursor.execute(
                "INSERT INTO source.counterparties (counterparty_id, counterparty_name, lei_code, risk_rating, counterparty_type, country, city) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                r["counterparty_id"], r["counterparty_name"], r["lei_code"],
                r["risk_rating"], r["counterparty_type"], r["country"], r["city"]
            )
    print(f"  Loaded counterparties.csv")


def main():
    parser = argparse.ArgumentParser(description="Load seed data into Azure SQL DB")
    parser.add_argument("--server", required=True, help="SQL Server hostname")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before loading")
    args = parser.parse_args()

    print(f"Connecting to {args.server}/{args.database}...")
    conn = get_connection(args)
    cursor = conn.cursor()

    if args.truncate:
        print("Truncating existing data...")
        for table in ["historical_trades", "counterparties", "traders", "instruments"]:
            cursor.execute(f"TRUNCATE TABLE source.{table}")
        conn.commit()

    print("Loading seed data...")
    load_instruments(cursor)
    load_traders(cursor)
    load_counterparties(cursor)
    conn.commit()

    # Verify counts
    for table in ["instruments", "traders", "counterparties"]:
        cursor.execute(f"SELECT COUNT(*) FROM source.{table}")
        count = cursor.fetchone()[0]
        print(f"  source.{table}: {count} rows")

    conn.close()
    print("\nSeed data loaded successfully!")


if __name__ == "__main__":
    main()
