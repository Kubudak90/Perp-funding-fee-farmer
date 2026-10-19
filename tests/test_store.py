from decimal import Decimal as D

from farmer.models import Fill, FundingPayment, Side
from farmer.persistence.store import Store


def test_store_summary_and_pnl(tmp_path):
    st = Store(tmp_path / "f.db")
    st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), fee=D("0"), is_maker=True, trade_id="a1", ts=10), "maker")
    st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), fee=D("0"), is_maker=True, trade_id="a1", ts=10), "maker")  # dup
    st.record_fill(Fill("lighter", "BTC", Side.SELL, D("100.1"), D("1"), fee=D("0"), is_maker=False, trade_id="l1", ts=11), "hedge")
    st.record_funding(FundingPayment("lighter", "BTC", D("0.5"), ts=12))
    s = st.summary(0)
    assert s["arcus"].volume == D("100") and s["arcus"].fills == 1
    assert s["lighter"].taker_volume == D("100.1")
    assert st.total_volume(0) == D("200.1")
    # hedged: net base 0 -> pnl = 0.1 spread + 0.5 funding
    assert st.pnl(0, {"BTC": D("105")}) == D("0.6")
    st.record_event("start", {"x": 1})
    assert st.recent_events(1)[0][1] == "start"
