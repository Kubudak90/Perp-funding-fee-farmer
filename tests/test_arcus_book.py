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


def test_crossing_stale_snapshot_level_is_dropped():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["100", "1"]], "asks": [["101", "1"]], "lastSequenceId": 1})
    # a fresh bid at 101 proves the (never confirmed) snapshot ask at 101 is gone
    assert book.apply_delta({"bids": [["101", "1"]], "asks": [], "lastSequenceId": 2}) is DeltaResult.APPLIED
    assert book.best_bid == (D("101"), D("1")) and book.best_ask is None and book.repairs == 1


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


# ---------------------------------------------------------------------------------------------------
# Live finding: the documented first-delta "boundary gap" is a LOSS window. Changes between the
# snapshot's lastSequenceId and the first delta are never delivered, so some snapshot levels are stale.
def replay(name: str, *, repair: bool):
    book = None
    out = {"crossed": 0, "bbo_false": 0, "bbo_true": 0, "applied": 0}
    for f in load_jsonl(name):
        c = f["contents"]
        if f["channel"] == "l2OrderbookUpdates":
            if f["type"] == "subscribed":
                book = L2Book("X", f["id"])
                book.apply_snapshot(c)
                continue
            if not repair:
                book._unconfirmed.clear()  # behave like a naive snapshot+delta book
            r = book.apply_delta(c)
            if r is DeltaResult.CROSSED:
                out["crossed"] += 1
                if not repair:
                    return out, book
            elif r is DeltaResult.APPLIED:
                out["applied"] += 1
        elif f["channel"] == "bbo" and book is not None:
            chk = book.reconcile_bbo(c) if repair else book.check_bbo(c)
            if chk is False:
                out["bbo_false"] += 1
            elif chk is True:
                out["bbo_true"] += 1
                assert book.check_bbo(c) is True
    return out, book


def test_eth_loss_window_stale_ask_is_repaired_from_bbo():
    naive, _ = replay("book_eth_losswindow.jsonl", repair=False)
    assert naive["bbo_false"] > 0  # a stale snapshot ask (2405.09) survives in a naive book
    fixed, book = replay("book_eth_losswindow.jsonl", repair=True)
    assert fixed["bbo_false"] == 0 and fixed["crossed"] == 0 and fixed["bbo_true"] > 0
    assert book.repairs >= 1 and book.synced
    assert D("2405.09") not in {lv.price for lv in book.top(50)[1]}


def test_sol_loss_window_cross_is_repaired():
    naive, _ = replay("book_sol_losswindow.jsonl", repair=False)
    assert naive["crossed"] == 1  # a stale snapshot ask crosses a fresh bid
    fixed, book = replay("book_sol_losswindow.jsonl", repair=True)
    assert fixed["crossed"] == 0 and fixed["bbo_false"] == 0 and book.repairs >= 1
    assert not book.is_crossed()


def _book_after_first_delta() -> L2Book:
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["99", "1"], ["98", "1"]], "asks": [["101", "1"], ["102", "2"]], "lastSequenceId": 10})
    assert book.apply_delta({"bids": [["97", "3"]], "asks": [], "lastSequenceId": 20}) is DeltaResult.APPLIED
    return book


def test_reconcile_bbo_rules():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["99", "1"]], "asks": [["101", "1"]], "lastSequenceId": 10})
    bbo0 = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "102", "size": "2"}, "lastSequenceId": 10}
    assert book.reconcile_bbo(bbo0) is None  # before the first delta: as stale as the snapshot

    # stale snapshot ask 101 (filled in the loss window) is dropped; the top then matches the bbo
    book = _book_after_first_delta()
    bbo = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "102", "size": "2"}, "lastSequenceId": 20}
    assert book.reconcile_bbo(bbo) is True
    assert book.best_ask == (D("102"), D("2")) and book.repairs == 1 and book.check_bbo(bbo)

    # a level inserted during the loss window is added; a stale (unconfirmed) size is corrected
    book = _book_after_first_delta()
    bbo = {"bestBid": {"price": "99.5", "size": "4"}, "bestAsk": {"price": "101", "size": "1.5"}, "lastSequenceId": 20}
    assert book.reconcile_bbo(bbo) is True
    assert book.best_bid == (D("99.5"), D("4")) and book.best_ask == (D("101"), D("1.5")) and book.repairs == 2

    # disagreement with a delta-confirmed level means the stream is broken
    book = _book_after_first_delta()
    assert book.apply_delta({"bids": [], "asks": [["100.5", "1"]], "lastSequenceId": 21}) is DeltaResult.APPLIED
    bad = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "101", "size": "1"}, "lastSequenceId": 21}
    assert book.reconcile_bbo(bad) is False
    assert book.reconcile_bbo({**bad, "lastSequenceId": 99}) is None  # different sequence: not comparable
    ok = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "100.5", "size": "1"}, "lastSequenceId": 21}
    assert book.reconcile_bbo(ok) is True


def test_crossed_by_confirmed_levels_still_fails():
    book = L2Book("X", "X-USD")
    book.apply_snapshot({"bids": [["100", "1"]], "asks": [["105", "1"]], "lastSequenceId": 1})
    assert book.apply_delta({"bids": [], "asks": [["103", "1"]], "lastSequenceId": 2}) is DeltaResult.APPLIED
    assert book.apply_delta({"bids": [["103.5", "1"]], "asks": [], "lastSequenceId": 3}) is DeltaResult.CROSSED


def test_prune_with_older_bbo_past_the_loss_window():
    """Quiet market: the snapshot ask 101 was lifted inside the loss window (11..19) and deltas only touch
    deep levels afterwards, so no same-sequence bbo ever arrives. An OLDER bbo from past the window
    still proves 101 stale and fixes the size of the real best ask."""
    book = _book_after_first_delta()  # snapshot seq 10, first delta seq 20
    assert book.apply_delta({"bids": [["96", "1"]], "asks": [], "lastSequenceId": 21}) is DeltaResult.APPLIED
    assert book.apply_delta({"bids": [], "asks": [["103", "1"]], "lastSequenceId": 22}) is DeltaResult.APPLIED
    assert book.top_unconfirmed() and book.best_ask == (D("101"), D("1"))
    bbo = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "102", "size": "2.5"}, "lastSequenceId": 20}
    assert book.prune_with_bbo(bbo) is True
    assert book.best_ask == (D("102"), D("2.5")) and book.best_bid == (D("99"), D("1")) and book.repairs == 2
    assert not book.top_unconfirmed()

    # a bbo from INSIDE the loss window proves nothing (101 may have come back later in the window)
    book = _book_after_first_delta()
    inside = dict(bbo, lastSequenceId=15)
    assert book.prune_with_bbo(inside) is None and book.best_ask == (D("101"), D("1"))
    assert book.prune_with_bbo(dict(bbo, lastSequenceId=9)) is None  # older than the snapshot
    assert book.prune_with_bbo(dict(bbo, lastSequenceId=25)) is None  # newer than the book
    same = {"bestBid": {"price": "99", "size": "1"}, "bestAsk": {"price": "102", "size": "2"}, "lastSequenceId": 20}
    assert book.prune_with_bbo(same) is True and book.best_ask == (D("102"), D("2"))  # exact: reconcile_bbo

    # a CONFIRMED level better than an older bbo was added after it and is kept
    book = _book_after_first_delta()
    assert book.apply_delta({"bids": [["99.5", "1"]], "asks": [], "lastSequenceId": 21}) is DeltaResult.APPLIED
    assert book.prune_with_bbo(bbo) is True and book.best_bid == (D("99.5"), D("1"))
    assert book.best_ask == (D("102"), D("2.5"))
    # before the first delta nothing is known about the loss window
    fresh = L2Book("X", "X-USD")
    fresh.apply_snapshot({"bids": [["99", "1"]], "asks": [["101", "1"]], "lastSequenceId": 10})
    assert fresh.prune_with_bbo(dict(bbo, lastSequenceId=10)) is None and fresh.best_ask == (D("101"), D("1"))
