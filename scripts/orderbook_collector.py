#!/usr/bin/env python3
"""
Orderbook collector for BTC perpetuals with REST snapshot + diff-apply (Binance Futures).

Behavior:
- Connect to Binance Futures depth websocket stream and buffer incoming diffs.
- Fetch a REST depth snapshot, align websocket diffs, and apply diffs to maintain
  a local orderbook in memory.
- Write raw events to `data/` and periodic full snapshots as JSON lines.

This implements Binance's recommended algorithm for maintaining a local orderbook:
1. Open a stream to the depth stream and buffer incoming events.
2. Get a REST snapshot from the REST API (`/fapi/v1/depth`).
3. Drop any buffered events with `u` < `lastUpdateId` from the snapshot.
4. Find the first event where `U <= lastUpdateId+1 <= u` and apply it and following events.

Notes:
- This is a simple, initial implementation intended for experimentation and
  small-scale collection. For production use add persistence, validation, and
  stronger error handling.
"""

import asyncio
import json
import os
import time
from datetime import datetime
import argparse
import gzip
import shutil

try:
    import websockets
except Exception:
    raise RuntimeError("Missing dependency 'websockets'. Install with: pip install websockets")

try:
    import aiohttp
except Exception:
    raise RuntimeError("Missing dependency 'aiohttp'. Install with: pip install aiohttp")


DEFAULT_DATA_DIR = r"C:\Users\Spike\OneDrive - National University of Singapore\Desktop\NUS\Improving my Coding\Codes\Market Microstructure Alpha + Execution Simulator\Short-Horizon-Market-Microstructure-Alpha\data"


class OrderBook:
    def __init__(self):
        # store price->qty as strings to preserve precision
        self.bids = {}  # price -> qty
        self.asks = {}

    @staticmethod
    def _apply_side(side_dict, updates):
        for price_str, qty_str in updates:
            if float(qty_str) == 0:
                side_dict.pop(price_str, None)
            else:
                side_dict[price_str] = qty_str

    def apply_snapshot(self, snapshot):
        self.bids = {p: q for p, q in snapshot.get("bids", [])}
        self.asks = {p: q for p, q in snapshot.get("asks", [])}

    def apply_diff_event(self, ev):
        # ev expected to have 'b' (bids) and 'a' (asks) in Binance depth format
        if "b" in ev:
            self._apply_side(self.bids, ev["b"])
        if "a" in ev:
            self._apply_side(self.asks, ev["a"])

    def to_dict(self):
        return {"bids": list(self.bids.items()), "asks": list(self.asks.items())}


async def fetch_rest_snapshot(session, symbol: str):
    # Binance Futures REST endpoint
    url = "https://fapi.binance.com/fapi/v1/depth"
    params = {"symbol": symbol, "limit": 1000}
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        data = await resp.json()
        # expected keys: lastUpdateId, bids, asks
        return data


async def collect_binance(symbol: str, depth: int, data_dir: str):
    
    base_ws = "wss://fstream.binance.com/ws"
    # Use diff depth stream (not partial depth) so REST + diff sync logic with U/u is valid
    stream = f"{symbol.lower()}@depth@100ms"
    url = f"{base_ws}/{stream}"

    os.makedirs(data_dir, exist_ok=True)
    raw_out_path = os.path.join(data_dir, f"binance_{symbol}_depth{depth}_events.jsonl")
    snapshot_out_path = os.path.join(data_dir, f"binance_{symbol}_orderbook_snapshots.jsonl")
    # compression settings (seconds)
    compress_interval = 60  # how often to check for old files
    compress_age = 300  # compress files older than this (seconds)

    # queue for incoming websocket messages
    queue = asyncio.Queue()

    async def ws_reader():
        print(f"[ws_reader] Starting websocket reader for {url}")
        backoff = 1
        while True:
            try:
                print(f"[ws_reader] Attempting connection with 30s timeout...")
                try:
                    ws = await asyncio.wait_for(websockets.connect(url, ping_interval=20, ping_timeout=10), timeout=30.0)
                except asyncio.TimeoutError:
                    print(f"[ws_reader] Connection timed out after 30s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                async with ws:
                    print(f"Connected to Binance depth stream: {stream}")
                    backoff = 1
                    msg_count = 0
                    async for msg in ws:
                        msg_count += 1
                        if msg_count == 1:
                            print(f"[ws_reader] First message received!")
                        try:
                            now = datetime.utcnow().isoformat() + "Z"
                            obj = json.loads(msg)
                            # save raw
                            record = {"received_at": now, "exchange": "binance", "symbol": symbol, "depth": depth, "data": obj}
                            with open(raw_out_path, "a", encoding="utf-8") as f:
                                f.write(json.dumps(record) + "\n")
                            if msg_count % 100 == 0:
                                print(f"[ws_reader] Received {msg_count} messages")
                        except Exception as e:
                            print(f"[ws_reader] Error writing event: {e}")
                        await queue.put(obj)
            except Exception as exc:
                print(f"[ws_reader] Connection error: {exc}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # start websocket reader task
    ws_task = asyncio.create_task(ws_reader())

    # buffer incoming events for a short time while we fetch snapshot
    buffer = []
    start = time.time()
    while time.time() - start < 1.0:
        try:
            ev = await asyncio.wait_for(queue.get(), timeout=1.0)
            buffer.append(ev)
        except asyncio.TimeoutError:
            break

    # fetch REST snapshot
    async with aiohttp.ClientSession() as session:
        snapshot = await fetch_rest_snapshot(session, symbol)

    last_update_id = snapshot.get("lastUpdateId")
    print(f"Fetched REST snapshot lastUpdateId={last_update_id}; buffered {len(buffer)} events")

    ob = OrderBook()
    ob.apply_snapshot(snapshot)

    # per Binance: drop any event where u < lastUpdateId
    # find first event where U <= lastUpdateId+1 <= u
    # apply that event and subsequent events in order
    applied = False
    pending = []
    # combine buffered events and then continue reading queue
    # process buffer first
    for ev in buffer:
        # expected Binance diff fields: U, u, b, a
        try:
            U = ev.get("U")
            u = ev.get("u")
        except Exception:
            U = None
            u = None
        if U is None or u is None:
            # not a diff depth message; skip applying
            continue
        if u < last_update_id:
            continue
        if not applied and U <= last_update_id + 1 <= u:
            # this is the first usable event
            ob.apply_diff_event(ev)
            applied = True
        elif applied:
            ob.apply_diff_event(ev)

    # if we didn't find a matching buffered event, keep reading until we find it (with timeout)
    if not applied:
        print("No matching buffered event found; scanning incoming stream for starting update (10s timeout)...")
        scan_start = time.time()
        scan_timeout = 10  # seconds
        while time.time() - scan_start < scan_timeout and not applied:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                U = ev.get("U")
                u = ev.get("u")
            except Exception:
                continue
            if U is None or u is None:
                continue
            if u < last_update_id:
                continue
            if U <= last_update_id + 1 <= u:
                ob.apply_diff_event(ev)
                applied = True
                break
        
        if not applied:
            print(f"Alignment scan timed out after {scan_timeout}s; initializing orderbook from REST snapshot without stream diffs.")

    # apply remaining queued events (non-blocking), filtering by lastUpdateId
    while not queue.empty():
        ev = await queue.get()
        try:
            u = ev.get("u")
        except Exception:
            continue
        # only apply if u >= last_update_id (or if we skipped alignment, any event is ok)
        if applied or u >= last_update_id:
            ob.apply_diff_event(ev)

    print("Orderbook synced with stream. Now applying live updates and persisting snapshots.")

    # background writer: periodically write full snapshot to file
    async def snapshot_writer():
        while True:
            try:
                await asyncio.sleep(5)
                out = {"written_at": datetime.utcnow().isoformat() + "Z", "symbol": symbol, "orderbook": ob.to_dict()}
                with open(snapshot_out_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(out) + "\n")
                print(f"[snapshot_writer] Wrote snapshot ({len(ob.bids)} bids, {len(ob.asks)} asks) to {snapshot_out_path}")
            except Exception as e:
                print(f"[snapshot_writer] Error writing snapshot: {e}")

    snap_task = asyncio.create_task(snapshot_writer())

    async def compress_old_files(dirpath: str, interval: int = 60, older_than: int = 300):
        while True:
            try:
                now_ts = time.time()
                for name in os.listdir(dirpath):
                    if not name.endswith('.jsonl'):
                        continue
                    path = os.path.join(dirpath, name)
                    try:
                        mtime = os.path.getmtime(path)
                    except Exception:
                        continue
                    age = now_ts - mtime
                    gz_path = path + '.gz'
                    if age >= older_than and not os.path.exists(gz_path):
                        # compress to .gz
                        try:
                            with open(path, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                            os.remove(path)
                            print(f"Compressed {path} -> {gz_path}")
                        except Exception as e:
                            print(f"Failed to compress {path}: {e}")
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Compression task error: {e}")
                await asyncio.sleep(interval)

    compress_task = asyncio.create_task(compress_old_files(data_dir, compress_interval, compress_age))

    # main loop: consume queue and apply diffs in real time
    try:
        while True:
            ev = await queue.get()
            # only apply if event has u field
            if "u" in ev:
                ob.apply_diff_event(ev)
    except asyncio.CancelledError:
        pass
    finally:
        ws_task.cancel()
        snap_task.cancel()
        compress_task.cancel()


async def main():
    parser = argparse.ArgumentParser(description="Orderbook collector (Binance futures depth) with snapshot+diff-apply")
    parser.add_argument("--exchange", default="binance", choices=["binance"], help="Exchange to connect to (initially only 'binance')")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol to subscribe to (e.g. BTCUSDT)")
    parser.add_argument("--depth", type=int, default=20, help="Depth level for depth stream (5,10,20)")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Folder to store data files")

    args = parser.parse_args()

    if args.exchange == "binance":
        await collect_binance(args.symbol, args.depth, args.data_dir)
    else:
        print("Exchange not implemented yet")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user")
