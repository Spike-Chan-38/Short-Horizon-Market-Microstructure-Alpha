# Initial Orderbook Data Pipeline

This contains a small, initial pipeline to collect BTC perpetual orderbook depth from Binance Futures and store raw messages in the `data/` folder.

Usage

- Install dependencies:
```bash
pip install -r requirements.txt
```

- Run collector (defaults to Binance BTCUSDT depth20):
```bash
python scripts/orderbook_collector.py
```

- Run collector with options (example):
```bash
python scripts/orderbook_collector.py --symbol BTCUSDT --depth 20 --data-dir data
```

Where the script writes JSON lines files to `data/` (created if missing):

- Raw events: `binance_BTCUSDT_depth20_events.jsonl`
- Periodic full snapshots: `binance_BTCUSDT_orderbook_snapshots.jsonl`

Next steps / notes

- This collector now implements a REST snapshot + diff-apply workflow to maintain a correct local orderbook. The snapshot file contains full orderbook dumps written periodically; raw events are also saved.
- To add other exchanges (Bybit, OKX), implement exchange-specific websocket + REST snapshot handlers and add to the CLI.
