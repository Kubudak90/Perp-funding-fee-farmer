"""TxExecutor (spec §11.1): nonce stream, rejections, lost acks, identical resends, REST fallback."""

import asyncio
import json

import pytest

from farmer.exchanges.lighter.helpers import TxNotSent, TxRejected, TxUnknown
from farmer.exchanges.lighter.signer import SignedTx, TxExecutor


class FakeChain:
    """Minimal server model: strict prev+1 nonces, tx index by hash."""

    def __init__(self, nonce: int = 100) -> None:
        self.nonce = nonce
        self.accepted: dict[str, int] = {}  # hash -> nonce

    def accept(self, tx: SignedTx) -> dict:
        if tx.tx_hash in self.accepted:
            return {"code": 21104, "message": "invalid nonce"}
        if tx.nonce != self.nonce:
            return {"code": 21104, "message": "invalid nonce"}
        self.nonce += 1
        self.accepted[tx.tx_hash] = tx.nonce
        return {"code": 200, "tx_hash": tx.tx_hash, "predicted_execution_time_ms": 0}


class FakeTransport:
    def __init__(self, chain: FakeChain, *, ws: bool = True) -> None:
        self.chain = chain
        self.ws = ws
        self.ws_script: list = []  # per-call behaviours for send_ws (default: accept)
        self.rest_script: list = []
        self.calls: list[tuple] = []
        self.lookups = 0
        self.nonce_fetches = 0
        self.lookup_hidden = 0  # number of lookups that pretend "not indexed yet"

    def ws_ready(self) -> bool:
        return self.ws

    async def _run(self, script, tx):
        action = script.pop(0) if script else "accept"
        if action == "accept":
            return self.chain.accept(tx)
        if action == "lost":  # never reached the server, no ack
            raise asyncio.TimeoutError
        if action == "accepted_no_ack":
            self.chain.accept(tx)
            raise asyncio.TimeoutError
        if action == "closed_after_accept":
            self.chain.accept(tx)
            raise ConnectionError("ws closed")
        if action == "closed":
            raise ConnectionError("ws closed")
        if action == "not_sent":
            raise TxNotSent("down")
        if isinstance(action, tuple) and action[0] == "accept_then":  # executed, but answered with action[1]
            self.chain.accept(tx)
            return action[1]
        if isinstance(action, dict):
            return action
        raise AssertionError(action)

    async def send_ws(self, tx, timeout):
        self.calls.append(("ws", tx.nonce, tx.tx_hash))
        return await self._run(self.ws_script, tx)

    async def send_rest(self, tx, timeout):
        self.calls.append(("rest", tx.nonce, tx.tx_hash))
        return await self._run(self.rest_script, tx)

    async def lookup_tx(self, tx_hash):
        self.lookups += 1
        if self.lookup_hidden > 0:
            self.lookup_hidden -= 1
            return None
        if tx_hash in self.chain.accepted:
            return {"code": 200, "hash": tx_hash, "status": 2}
        return None

    async def fetch_next_nonce(self):
        self.nonce_fetches += 1
        return self.chain.nonce


def builder(intent: str):
    """Deterministic 'signer': hash depends on intent + nonce (like the real signer)."""
    built: list[SignedTx] = []

    def build(nonce: int) -> SignedTx:
        tx = SignedTx(14, json.dumps({"Nonce": nonce, "intent": intent}), f"{intent}-{nonce}".ljust(80, "0"), nonce, intent)
        built.append(tx)
        return tx

    build.built = built  # type: ignore[attr-defined]
    return build


def make(chain=None, **kw):
    chain = chain or FakeChain()
    t = FakeTransport(chain, **kw)
    ex = TxExecutor(t, ack_timeout_s=0.05, rest_timeout_s=0.05, lookup_tries=2, lookup_interval_s=0.0)
    return ex, t, chain


async def test_normal_flow_increments_local_nonce():
    ex, t, chain = make()
    r1 = await ex.submit(builder("a"))
    r2 = await ex.submit(builder("b"))
    assert (r1.nonce, r2.nonce) == (100, 101) and r1.via == "ws"
    assert ex.next_nonce == 102 == chain.nonce
    assert t.nonce_fetches == 1  # fetched once at first use, never per tx


async def test_invalid_nonce_resyncs_and_resigns_once():
    ex, t, chain = make(FakeChain(105))
    ex.next_nonce = 100  # stale local nonce
    b = builder("a")
    r = await ex.submit(b)
    assert r.nonce == 105 and [tx.nonce for tx in b.built] == [100, 105]
    assert ex.next_nonce == 106


async def test_api_rejection_does_not_consume_nonce():
    ex, t, chain = make()
    t.ws_script = [{"error": {"code": 21701, "message": "invalid base amount"}, "id": "x"}]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("bad"))
    assert ei.value.tx_code == 21701 and not ei.value.retryable
    assert ex.next_nonce == 100
    r = await ex.submit(builder("good"))
    assert r.nonce == 100


async def test_restricted_is_fatal_and_not_retryable():
    ex, t, _ = make()
    t.ws_script = [{"code": 20558, "message": "You are accessing Lighter from a restricted jurisdiction"}]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("a"))
    assert ei.value.fatal and not ei.value.retryable


async def test_lost_ack_not_accepted_resends_identical_payload():
    ex, t, chain = make()
    t.ws_script = ["lost"]
    b = builder("hedge")
    r = await ex.submit(b)
    assert r.resent and len(b.built) == 1  # never re-signed
    sends = [c for c in t.calls if c[0] in ("ws", "rest")]
    assert len(sends) == 2 and sends[0][2] == sends[1][2]  # identical tx hash
    assert chain.accepted == {b.built[0].tx_hash: 100} and ex.next_nonce == 101


async def test_accepted_without_ack_resolved_by_lookup_no_duplicate():
    ex, t, chain = make()
    t.ws_script = ["accepted_no_ack"]
    b = builder("hedge")
    r = await ex.submit(b)
    assert r.via == "lookup" and not r.resent and len(b.built) == 1
    assert len([c for c in t.calls if c[0] == "ws"]) == 1
    assert ex.next_nonce == 101 == chain.nonce


async def test_ws_closed_falls_back_to_rest_with_same_payload():
    ex, t, chain = make()
    t.ws_script = ["closed"]
    b = builder("hedge")
    r = await ex.submit(b)
    assert r.via == "rest" and [c[0] for c in t.calls] == ["ws", "rest"] and t.calls[0][2] == t.calls[1][2]
    assert len(b.built) == 1 and chain.nonce == 101


async def test_ws_closed_after_accept_then_rest_invalid_nonce_is_resolved_not_resigned():
    ex, t, chain = make()
    t.ws_script = ["closed_after_accept"]  # first copy landed, ack lost with the socket
    b = builder("hedge")
    r = await ex.submit(b)  # REST copy gets 21104 -> must NOT re-sign; lookup finds the first copy
    assert len(b.built) == 1 and r.via == "lookup"
    assert ex.next_nonce == 101 == chain.nonce


async def test_ws_down_uses_rest():
    ex, t, chain = make(ws=False)
    r = await ex.submit(builder("a"))
    assert r.via == "rest" and [c[0] for c in t.calls] == ["rest"]


async def test_nothing_sent_raises_retryable_rejection():
    ex, t, _ = make()
    t.ws_script = ["not_sent"]
    t.rest_script = ["not_sent"]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("a"))
    assert ei.value.retryable and ex.next_nonce == 100


async def test_unknown_when_nonce_moved_and_tx_not_found():
    chain = FakeChain()
    ex, t, _ = make(chain)
    t.ws_script = ["lost"]
    orig = t.fetch_next_nonce

    async def moved():
        chain.nonce = 103  # someone else consumed nonces / or lagging index
        return await orig()

    t.fetch_next_nonce = moved  # type: ignore[assignment]
    ex.next_nonce = 100
    with pytest.raises(TxUnknown):
        await ex.submit(builder("hedge"))
    assert ex.next_nonce == 103


async def test_identical_resend_gets_invalid_nonce_then_found():
    """First copy lands late (not indexed during the first lookups); resend gets 21104; second lookup finds it."""
    ex, t, chain = make()
    t.ws_script = ["accepted_no_ack"]
    t.lookup_hidden = 2  # first lookup round sees nothing
    orig_fetch = t.fetch_next_nonce

    async def lagging():  # nextNonce still shows our nonce (speed bump lag)
        await orig_fetch()
        return 100

    t.fetch_next_nonce = lagging  # type: ignore[assignment]
    b = builder("hedge")
    r = await ex.submit(b)
    assert len(b.built) == 1 and r.via == "lookup"
    assert list(chain.accepted.values()) == [100]  # executed exactly once


async def test_sequencer_failure_status_zero():
    ex, t, chain = make()
    t.ws_script = ["accepted_no_ack"]

    async def lookup(h):
        return {"code": 200, "hash": h, "status": 0}

    t.lookup_tx = lookup  # type: ignore[assignment]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("a"))
    assert ei.value.tx_code == 0


async def test_serializes_concurrent_submits():
    ex, t, chain = make()
    rs = await asyncio.gather(*(ex.submit(builder(f"x{i}")) for i in range(5)))
    assert sorted(r.nonce for r in rs) == [100, 101, 102, 103, 104] and chain.nonce == 105


async def test_nonce_fetch_failure_before_send_is_retryable():
    ex, t, _ = make()

    async def boom():
        raise RuntimeError("down")

    t.fetch_next_nonce = boom  # type: ignore[assignment]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("a"))
    assert ei.value.retryable and t.calls == []


# --------------------------------------------------------------------------- review fixes
async def test_ambiguous_ack_after_accept_resolved_by_hash_no_double_execution():
    """29501 'process timeout' does not prove the tx was dropped: resolve by hash, never report 'not executed'."""
    ex, t, chain = make()
    t.ws_script = [("accept_then", {"error": {"code": 29501, "message": "process timeout"}, "id": "x"})]
    b1 = builder("hedge1")
    r1 = await ex.submit(b1)
    assert r1.via == "lookup" and len(b1.built) == 1 and ex.next_nonce == 101
    r2 = await ex.submit(builder("hedge2"))
    assert r2.nonce == 101
    assert sorted(chain.accepted.values()) == [100, 101]  # hedge1 once, hedge2 once


async def test_ambiguous_ack_not_executed_resends_identical_payload():
    ex, t, chain = make()
    t.ws_script = [{"code": 29500, "message": "internal server error"}]
    b = builder("hedge")
    r = await ex.submit(b)
    assert r.resent and len(b.built) == 1
    assert chain.accepted == {b.built[0].tx_hash: 100} and ex.next_nonce == 101


@pytest.mark.parametrize("code", [21506, 29500, 23000, -403])
async def test_maybe_dup_rest_copy_transient_rejection_is_resolved(code):
    """WS died after the frame left (copy executed); the REST copy is refused transiently -> resolve, not reject."""
    ex, t, chain = make()
    t.ws_script = ["closed_after_accept"]
    t.rest_script = [{"code": code, "message": "transient"}]
    b = builder("hedge")
    r = await ex.submit(b)
    assert r.via == "lookup" and len(b.built) == 1 and ex.next_nonce == 101
    await ex.submit(builder("next"))
    assert sorted(chain.accepted.values()) == [100, 101]  # the hedge executed exactly once


async def test_maybe_dup_deterministic_rejection_is_not_resolved():
    ex, t, chain = make()
    t.ws_script = ["closed"]
    t.rest_script = [{"code": 21701, "message": "invalid base amount"}]
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("bad"))
    assert ei.value.tx_code == 21701 and t.lookups == 0 and ex.next_nonce == 100


@pytest.mark.parametrize("resend", [{"code": 21506, "message": "too many pending txs"},
                                    ("accept_then", {"code": 29500, "message": "internal server error"})])
async def test_identical_resend_transient_answer_is_unknown(resend):
    """The first copy's outcome is unknown by definition: a transient answer to the resend cannot settle it."""
    ex, t, chain = make()
    t.ws_script = ["lost", resend]
    with pytest.raises(TxUnknown):
        await ex.submit(builder("hedge"))
    assert ex.next_nonce is None and ex.stats["unknown"] == 1


async def test_second_invalid_nonce_after_resync_is_retryable():
    """nextNonce lags while an earlier tx sits in the speed bump: a second 21104 is transient."""
    chain = FakeChain(101)  # tx 100 accepted, still pending
    ex, t, _ = make(chain)
    ex.next_nonce = 100
    t.fetch_next_nonce = lambda: _const(100)  # type: ignore[assignment]  # lagging nextNonce
    with pytest.raises(TxRejected) as ei:
        await ex.submit(builder("hedge"))
    assert ei.value.tx_code == 21104 and ei.value.retryable and ex.next_nonce is None


async def _const(n):
    return n


async def test_request_resync_rereads_nonce_before_next_tx():
    """A sequencer failure (status 0) does not consume the nonce: the next tx must be signed with nextNonce."""
    ex, t, chain = make()
    r1 = await ex.submit(builder("a"))
    assert r1.nonce == 100 and ex.next_nonce == 101
    chain.nonce = 100  # tx 100 failed in the sequencer (status 0): nonce not consumed
    ex.request_resync()
    r2 = await ex.submit(builder("b"))
    assert r2.nonce == 100 and t.nonce_fetches == 2


async def test_request_resync_during_submit_is_not_lost():
    ex, t, chain = make()
    ex.next_nonce = 100
    gate = asyncio.Event()
    orig = t.send_ws

    async def slow(tx, timeout):
        await gate.wait()
        return await orig(tx, timeout)

    t.send_ws = slow  # type: ignore[assignment]
    task = asyncio.create_task(ex.submit(builder("a")))
    await asyncio.sleep(0.01)
    ex.request_resync()  # an earlier tx reported status 0 while this one is in flight
    gate.set()
    await task
    assert ex.next_nonce == 101
    fetches = t.nonce_fetches
    await ex.submit(builder("b"))
    assert t.nonce_fetches == fetches + 1
