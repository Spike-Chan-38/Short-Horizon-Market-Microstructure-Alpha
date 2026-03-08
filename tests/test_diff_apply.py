import sys
import os
import json

# ensure repo root is on path so we can import the script module
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.orderbook_collector import OrderBook


def test_diff_apply_sequence():
    # snapshot with lastUpdateId = 100
    snapshot = {
        "lastUpdateId": 100,
        "bids": [["100.0", "1.0"], ["99.0", "2.0"]],
        "asks": [["101.0", "1.0"], ["102.0", "2.0"]]
    }

    # events: first event has u < lastUpdateId (drop), second aligns
    ev1 = {"U": 95, "u": 99, "b": [["100.0", "0.9"]], "a": []}
    ev2 = {"U": 100, "u": 101, "b": [["100.0", "0.5"]], "a": [["101.0", "0"]]}
    ev3 = {"U": 102, "u": 103, "b": [["98.0", "1.5"]], "a": []}

    ob = OrderBook()
    ob.apply_snapshot(snapshot)

    # simulate dropping ev1 (u < lastUpdateId)
    # find first usable event: ev2 (U <= lastUpdateId+1 <= u) -> apply ev2 and subsequent
    # apply ev2
    ob.apply_diff_event(ev2)
    # apply ev3
    ob.apply_diff_event(ev3)

    # expected result after applying
    # bids: 100.0 -> 0.5, 99.0 -> 2.0, 98.0 -> 1.5
    # asks: 101.0 removed, 102.0 -> 2.0
    bids = dict(ob.bids)
    asks = dict(ob.asks)

    assert bids.get("100.0") == "0.5"
    assert bids.get("99.0") == "2.0"
    assert bids.get("98.0") == "1.5"
    assert "101.0" not in asks or float(asks.get("101.0", "0")) == 0
    assert asks.get("102.0") == "2.0"
