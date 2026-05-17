"""
scripts/generate_sample_data.py
--------------------------------
Generate synthetic trade and position data for pipeline testing.

Usage:
    python scripts/generate_sample_data.py --trades 50000 --positions 10000
    python scripts/generate_sample_data.py --trades 1000 --dirty 0.05  # 5% dirty records
"""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

random.seed(42)

# Reference data
CURRENCIES     = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]
DIRECTIONS     = ["BUY", "SELL"]
ASSET_CLASSES  = ["Equity", "FX", "Rates", "Credit", "Commodity"]
STATUSES       = ["SETTLED", "PENDING", "FAILED", "CANCELLED"]
COUNTERPARTIES = ["CP001", "CP002", "CP003", "CP004", "CP005"]
PRODUCTS       = ["PROD001", "PROD002", "PROD003", "PROD004", "PROD005",
                  "PROD006", "PROD007", "PROD008", "PROD009", "PROD010"]
EXCHANGES      = ["NYSE", "LSE", "TSE", "XETRA", "SGX", "ASX"]


def random_date(start_year: int = 2024, end_year: int = 2025) -> datetime:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_trade(trade_num: int, dirty_prob: float = 0.0) -> Dict[str, Any]:
    """Generate a single trade record, optionally injecting dirty data."""
    trade_date    = random_date()
    settle_offset = random.randint(1, 5) if random.random() > 0.02 else -1  # 2% bad settle
    settle_date   = trade_date + timedelta(days=settle_offset)

    notional = round(random.uniform(1_000, 50_000_000), 2)
    quantity = random.randint(1, 100_000)
    currency = random.choice(CURRENCIES)

    record = {
        "trade_id":         f"T_{trade_num:08d}",
        "product_id":       random.choice(PRODUCTS),
        "counterparty_id":  random.choice(COUNTERPARTIES),
        "asset_class":      random.choice(ASSET_CLASSES),
        "currency":         currency,
        "direction":        random.choice(DIRECTIONS),
        "notional":         notional,
        "quantity":         quantity,
        "trade_date":       trade_date.strftime("%Y-%m-%d %H:%M:%S"),
        "settlement_date":  settle_date.strftime("%Y-%m-%d %H:%M:%S"),
        "settlement_status": random.choice(STATUSES),
        "exchange":         random.choice(EXCHANGES),
    }

    # Inject dirty records
    if dirty_prob > 0 and random.random() < dirty_prob:
        fault = random.choice(["null_notional", "bad_currency", "negative_notional", "dup"])
        if fault == "null_notional":
            record["notional"] = ""
        elif fault == "bad_currency":
            record["currency"] = "XYZ"
        elif fault == "negative_notional":
            record["notional"] = round(-1 * notional, 2)
        elif fault == "dup":
            # Return same trade_id as a previous one — Dedup will catch this
            record["trade_id"] = f"T_{max(1, trade_num - random.randint(1, 50)):08d}"

    return record


def generate_position(pos_num: int) -> Dict[str, Any]:
    """Generate a single end-of-day position record."""
    return {
        "position_id":      f"POS_{pos_num:08d}",
        "product_id":       random.choice(PRODUCTS),
        "counterparty_id":  random.choice(COUNTERPARTIES),
        "asset_class":      random.choice(ASSET_CLASSES),
        "currency":         random.choice(CURRENCIES),
        "direction":        random.choice(DIRECTIONS),
        "quantity":         random.randint(100, 1_000_000),
        "market_value_usd": round(random.uniform(10_000, 100_000_000), 2),
        "as_of_date":       random_date().strftime("%Y-%m-%d"),
        "book":             f"BOOK_{random.randint(1, 20):02d}",
    }


def write_csv(records: List[Dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Written: {output_path}  ({len(records):,} records)")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic financial data")
    parser.add_argument("--trades",    type=int,   default=10_000,  help="Number of trade records")
    parser.add_argument("--positions", type=int,   default=2_000,   help="Number of position records")
    parser.add_argument("--dirty",     type=float, default=0.03,    help="Fraction of dirty records (0.0–1.0)")
    parser.add_argument("--outdir",    type=str,   default="data/raw", help="Output directory")
    args = parser.parse_args()

    ts     = datetime.utcnow().strftime("%Y%m%d")
    outdir = Path(args.outdir)

    print(f"\nGenerating {args.trades:,} trades ({args.dirty:.0%} dirty) ...")
    trades = [generate_trade(i, dirty_prob=args.dirty) for i in range(1, args.trades + 1)]
    write_csv(trades, outdir / f"trades_{ts}.csv")

    print(f"Generating {args.positions:,} positions ...")
    positions = [generate_position(i) for i in range(1, args.positions + 1)]
    write_csv(positions, outdir / f"positions_{ts}.csv")

    print(f"\nDone. Files written to: {outdir.resolve()}\n")


if __name__ == "__main__":
    main()