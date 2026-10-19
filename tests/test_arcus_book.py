"""L2 book maintenance replayed from a REAL captured BTC-USD stream (snapshot nLevels=100 + 150 deltas)."""

from decimal import Decimal as D

from farmer.exchanges.arcus.book import DeltaResult, L2Book, bbo_to_order_book, levels_to_order_book
from tests.test_arcus_support import load, load_jsonl


def stream():
    return load_jsonl("book_btc_stream.jsonl")


def seeded() -> tuple[L2Book, list[dict]]:
    frames = stream()
    book = L2Book("BTC", "BTC-USD")
    snap = next(f for f in frames if f["channel"] == "l2OrderbookUpdates" and f["type"] == "subscribed")
    book.apply_snapshot(snap["contents"], now=1.0)
    deltas = [f["contents"] for f in frames if f["channel"] == "l2OrderbookUpdates" and f["type"] == "channel_data"]
    return book, deltas


def test_replay_real_stream_with_bbo_cross_check():
    frames = stream()
    book = L2Book("BTC", "BTC-USD", depth=20)
    results, bbo_checks = [], []
    first_jump = None
    for f in frames:
        c = f["contents"]
        if f["channel"] == "l2OrderbookUpdates":
            if f["type"] == "subscribed":
                book.apply_snapshot(c)
                assert book.levels() == (100, 100)
                continue
            if first_jump is None:
                first_jump = int(c["lastSequenceId"]) - book.seq
            r = book.apply_delta(c)
            results.append(r)
            assert not book.is_crossed()
            ob = book.to_order_book()
            assert ob.best_bid < ob.best_ask
            assert [lv.price for lv in ob.bids] == sorted((lv.price for lv in ob.bids), reverse=True)
            assert [lv.price for lv in ob.asks] == sorted(lv.price for lv in ob.asks)
            assert len(ob.bids) == 20 and len(ob.asks) == 20 and ob.seq == book.seq
        elif f["channel"] == "bbo" and book.synced:
            chk = book.check_bbo(c)
            if chk is not None:
                bbo_checks.append(chk)
    assert first_jump > 1  # documented boundary gap: first delta is NOT snapshotSeq + 1
    assert set(results) == {DeltaResult.APPLIED} and len(results) == 150
    assert bbo_checks and all(bbo_checks)  # bbo shares the per-market sequence space and agrees with the book


def test_stale_and_unsynced_deltas_are_ignored():
    book = L2Book("BTC", "BTC-USD")
    assert book.apply_delta({"bids": [["1", "1"]], "asks": [], "lastSequenceId": 5}) is DeltaResult.UNSYNCED
    book, deltas = seeded()
    seq0 = book.seq
    stale = {"bids": [["1", "5"]], "asks": [], "lastSequenceId": seq0}
    assert book.apply_delta(stale) is DeltaResult.STALE
    assert book.seq == seq0 and D("1") not in dict((lv.price, 1) for lv in book.top(1000)[0])


def test_mid_stream_gap_requires_resnapshot():
    book, deltas = seeded()
    assert book.apply_delta(deltas[0]) is DeltaResult.APPLIED
    gap = dict(deltas[2])  # skip deltas[1]
    assert int(gap["lastSequenceId"]) > book.seq + 1
    assert book.apply_delta(gap) is DeltaResult.GAP
    assert not book.synced and book.gaps == 1
    assert book.apply_delta(deltas[3]) is DeltaResult.UNSYNCED


def test_absolute_sizes_zero_removes_and_last_write_wins():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["100", "1"], ["99", "2"]], "asks": [["101", "1"], ["102", "3"]], "lastSequenceId": 10})
    r = book.apply_delta({"bids": [["100", "0"], ["98", "4"], ["98", "5"]], "asks": [["101", "2"]], "lastSequenceId": 15})
    assert r is DeltaResult.APPLIED
    bids, asks = book.top(5)
    assert [(lv.price, lv.size) for lv in bids] == [(D("99"), D("2")), (D("98"), D("5"))]
    assert [(lv.price, lv.size) for lv in asks] == [(D("101"), D("2")), (D("102"), D("3"))]
    # contiguous from now on
    assert book.apply_delta({"bids": [], "asks": [["101", "0"]], "lastSequenceId": 16}) is DeltaResult.APPLIED
    assert book.best_ask == (D("102"), D("3"))
    assert book.apply_delta({"bids": [], "asks": [], "lastSequenceId": 18}) is DeltaResult.GAP


def test_crossed_after_delta_is_flagged():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["100", "1"]], "asks": [["101", "1"]], "lastSequenceId": 1})
    assert book.apply_delta({"bids": [["101", "1"]], "asks": [], "lastSequenceId": 2}) is DeltaResult.CROSSED
    assert not book.synced


def test_bbo_check_detects_mismatch():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["100", "1"]], "asks": [["101", "2"]], "lastSequenceId": 7})
    ok = {"bestBid": {"price": "100", "size": "1"}, "bestAsk": {"price": "101", "size": "2"}, "lastSequenceId": 7}
    assert book.check_bbo(ok) is True
    assert book.check_bbo({**ok, "lastSequenceId": 8}) is None
    assert book.check_bbo({**ok, "bestAsk": {"price": "101", "size": "2.5"}}) is False
    assert book.check_bbo({**ok, "bestBid": None}) is False


def test_trim_bounds_memory():
    book = L2Book("X", "X-USD", max_levels=10)
    book.apply_snapshot({"bids": [[str(100 - i), "1"] for i in range(5)], "asks": [[str(200 + i), "1"] for i in range(5)],
                         "lastSequenceId": 1})
    book.apply_delta({"bids": [[str(90 - i), "1"] for i in range(20)], "asks": [[str(300 + i), "1"] for i in range(20)],
                      "lastSequenceId": 2})
    assert book.levels() == (10, 10)
    assert book.best_bid[0] == D("100") and book.best_ask[0] == D("200")


def test_rest_and_bbo_conversions():
    ob = levels_to_order_book("BTC", load("rest_l2_btc.json"), depth=5)
    assert len(ob.bids) == 5 and ob.best_bid < ob.best_ask and ob.seq is not None
    frames = stream()
    bbo = next(f for f in frames if f["channel"] == "bbo")
    tob = bbo_to_order_book("BTC", bbo["contents"])
    assert len(tob.bids) == len(tob.asks) == 1 and tob.best_bid < tob.best_ask
    assert bbo_to_order_book("BTC", {"bestBid": None, "bestAsk": None}).mid is None
