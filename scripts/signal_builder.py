import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Allow running this file directly (e.g. `python scripts/signal_builder.py`) by
# ensuring the parent directory (which contains the `scripts/` package) is on PYTHONPATH.
_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

from scripts.functions.functions import VWPA_safe, orderbook_imbalance

ROOT = Path(__file__).resolve().parent.parent  # Short-Horizon-Market-Microstructure-Alpha/
raw_path = ROOT / "data" / "raw" / "binance_BTCUSDT_depth20_events.jsonl"
output_path = ROOT / "data" / "processed" / "btc.parquet"


def build_signals(raw_path, output_path):
    raw_path = Path(raw_path)
    output_path = Path(output_path)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data not found: {raw_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # insert here
    df = pd.read_json(raw_path, lines=True)
    # Convert bids and asks to NumPy arrays per row
    df["orderbook_bids"] = df["data"].map(lambda x: np.array(x["b"], dtype=float))
    df["orderbook_asks"] = df["data"].map(lambda x: np.array(x["a"], dtype=float))
    df["orderbook_asks"] = df["orderbook_asks"].map(
        lambda levels: [[float(p), float(q)] for p, q in levels]
    )
    df["orderbook_bids"] = df["orderbook_bids"].map(
        lambda levels: [[float(p), float(q)] for p, q in levels]
    )
    df["VWPA"] = df.apply(
        lambda row: VWPA_safe(row["orderbook_bids"], row["orderbook_asks"]),
        axis=1
    )

    # Calculating orderbook imbalance signals
    df["OBI"] = df.apply(
        lambda row: orderbook_imbalance(row["orderbook_bids"], row["orderbook_asks"]),
        axis=1
    )
    # Calculate best bid
    df["best_bid"] = df["orderbook_bids"].map(lambda b: b[0][0] if b else np.nan)
    # Calculate best ask
    df["best_ask"] = df["orderbook_asks"].map(lambda a: a[0][0] if a else np.nan)
    # Calculate mid point price (between best ask and best bid)
    df["mid_price"] = (df["best_bid"] + df["best_ask"]) * 0.5
    # More lagged variables
    df['best_ask_lag1'] = df["best_ask"].shift(1)
    df['best_ask_lag7'] = df["best_ask"].shift(7)
    df['best_ask_lag14'] = df["best_ask"].shift(14)
    df['best_bid_lag1'] = df["best_bid"].shift(1)
    df['best_bid_lag7'] = df["best_bid"].shift(7)
    df['best_bid_lag14'] = df["best_bid"].shift(14)

    print(f"Writing parquet to: {output_path}")

    df.to_parquet(output_path)
    return


if __name__ == "__main__":
    build_signals(raw_path=raw_path, output_path=output_path)
