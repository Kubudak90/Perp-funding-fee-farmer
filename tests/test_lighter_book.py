"""LocalBook against a captured live ETH stream (snapshot + 150 deltas + interleaved ticker BBO frames)."""

import json
from decimal import Decimal as D
from pathlib import Path

from farmer.exchanges.lighter.book import LocalBook

DATA = Path(__file__).parent / "data" / "lighter"


def frames():
    return [json.loads(line) for line in (DATA / "ws_order_book_eth.jsonl").read_text().splitlines() if line.strip()]


def test_replay_capture_continuity_and_bbo_matches_ticker():
    book = LocalBook("ETH", 0)
    all_frames = frames()
    tickers = {m["nonce"]: m["ticker"] for m in all_frames if m["channel"] == "ticker:0"}
    ticker_checks = 0
    for msg in all_frames:
        if msg["channel"] != "order_book:0":
            continue
        ob = msg["order_book"]
        if msg["type"] == "subscribed/order_book":
            book.apply_snapshot(ob)
        else:
            assert book.apply_update(ob), "live capture had no gaps"
        assert not book.crossed()
        t = tickers.get(book.nonce)  # ticker fires on every nonce; the book batches them every ~50 ms
        if t is not None:
            assert book.best_ask == D(t["a"]["price"]) and book.best_bid == D(t["b"]["price"])
            bids, asks = book.top(1)
            assert asks[0].size == D(t["a"]["size"]) and bids[0].size == D(t["b"]["size"])
            ticker_checks += 1
    assert book.updates == 150 and book.gaps == 0
    assert ticker_checks >= 5  # every shared nonce in the capture matched the ticker BBO exactly


def test_top_levels_sorted_and_zero_sizes_removed():
    book = LocalBook("ETH", 0)
    book.apply_snapshot({"nonce": 10, "begin_nonce": 0,
                         "asks": [{"price": "101.00", "size": "1"}, {"price": "100.50", "size": "2"}],
                         "bids": [{"price": "99.00", "size": "3"}, {"price": "99.50", "size": "4"}]})
    bids, asks = book.top(20)
    assert [lvl.price for lvl in asks] == [D("100.50"), D("101.00")]
    assert [lvl.price for lvl in bids] == [D("99.50"), D("99.00")]
    assert book.apply_update({"begin_nonce": 10, "nonce": 15, "asks": [{"price": "100.50", "size": "0.0000"}],
                              "bids": [{"price": "99.75", "size": "1"}, {"price": "99.00", "size": "0"}]})
    bids, asks = book.top(20)
    assert [lvl.price for lvl in asks] == [D("101.00")]
    assert [lvl.price for lvl in bids] == [D("99.75"), D("99.50")]
    # update of an existing level replaces its size (new total, not a delta)
    assert book.apply_update({"begin_nonce": 15, "nonce": 16, "bids": [{"price": "99.50", "size": "7"}], "asks": []})
    assert book.top(5)[0][1].size == D("7")
    ob = book.to_orderbook(depth=1)
    assert len(ob.bids) == 1 and ob.best_bid == D("99.75") and ob.best_ask == D("101.00") and ob.seq == 16


def test_gap_invalidates_until_new_snapshot():
    book = LocalBook("ETH", 0)
    book.apply_update({"begin_nonce": 1, "nonce": 2, "asks": [], "bids": []})  # before snapshot: ignored
    assert not book.valid
    book.apply_snapshot({"nonce": 100, "asks": [{"price": "2", "size": "1"}], "bids": [{"price": "1", "size": "1"}]})
    assert book.valid
    assert not book.apply_update({"begin_nonce": 99, "nonce": 120, "asks": [], "bids": []})
    assert not book.valid and book.gaps == 1
    # further deltas are ignored until a fresh snapshot arrives
    assert book.apply_update({"begin_nonce": 120, "nonce": 130, "asks": [{"price": "3", "size": "1"}], "bids": []})
    assert not book.valid
    book.apply_snapshot({"nonce": 200, "asks": [{"price": "2.5", "size": "1"}], "bids": []})
    assert book.valid and book.best_ask == D("2.5") and book.best_bid is None


def test_crossed_detection():
    book = LocalBook("X", 1)
    book.apply_snapshot({"nonce": 1, "asks": [{"price": "10", "size": "1"}], "bids": [{"price": "10", "size": "1"}]})
    assert book.crossed()
