# Architecture

Delta-neutral funding + volume farmer between **Arcus** (maker leg, 0 ppm maker fee) and
**Lighter** (taker/hedge leg, 0 fee on Standard accounts). Python 3.11+, single asyncio loop.

```
src/farmer/
  models.py              domain dataclasses (Decimal everywhere, signed positions, funding = fraction/hour)
  config.py              pydantic config (YAML + env secrets)
  exchanges/
    base.py              Exchange ABC (the ONLY thing the strategy talks to)
    arcus/               Arcus adapter: auth.py (Ed25519 signing), rest.py, ws.py, client.py -> ArcusExchange
    lighter/             Lighter adapter (wraps lighter-sdk SignerClient): client.py -> LighterExchange, ws.py
    paper/client.py      PaperExchange(inner: Exchange, venue_cfg, paper_cfg): live public data, simulated orders
  strategy/
    pricing.py           pure functions: maker quote price/size from both books + edge requirement
    funding.py           FundingMonitor: polls funding on both venues, picks direction with hysteresis
    hedger.py            Hedger: keeps maker_pos + taker_pos (+ in-flight) == 0 using taker IOC orders
    trader.py            PairTrader: per-market state machine OPENING -> HOLDING -> CLOSING -> ...
    engine.py            Engine: wires venues, maps markets, starts traders/monitors, shutdown
  risk/manager.py        RiskManager: staleness, divergence, margin, unhedged delta, daily loss, circuit breaker
  persistence/store.py   Store: SQLite (fills, funding payments, events, per-hour aggregates) + aggregate queries
  monitoring/
    stats.py             StatsReporter: periodic log/Telegram report (volume, fees, funding, pnl, positions)
    notifier.py          Notifier (Telegram or null)
  cli.py                 `farmer scan|run|status|close-all|stats|keygen`
```

## Core trade loop (per market)

1. **Direction** (FundingMonitor): with hourly funding rates `r_m` (maker venue) and `r_t`
   (taker venue), holding maker SHORT / taker LONG earns `r_m - r_t` per hour; maker LONG /
   taker SHORT earns `r_t - r_m`. Choose the better one; keep the current direction unless
   the other is better by `flip_hysteresis_apr`. Direction only changes when the maker
   position is (near) flat. `direction = +1` means maker LONG, `-1` maker SHORT.
2. **Goal**: `OPENING` goal = `direction * target_base`; `CLOSING` goal = `direction * floor_base`
   (`floor_base = churn_floor_frac * target_base`); `HOLDING` = no quote.
   `target_base = target_notional / mid`, frozen at the start of each phase.
3. **Maker quote** (pricing.compute_maker_quote): side = sign(goal - maker_pos), size =
   min(clip, |goal - maker_pos|) rounded to the COMMON step of both venues
   (`utils.decimals.common_step`) and >= both venues' minimums. The hedge price is the taker
   book VWAP for that size on the opposite side. Edge-limit price (maker BUY):
   `max_px = (hedge_vwap*(1 - f_t) - min_edge_bps*1e-4*mid) / (1 + f_m)`; maker SELL is symmetric
   `min_px = (hedge_vwap*(1 + f_t) + min_edge_bps*1e-4*mid) / (1 - f_m)`.
   Final BUY price = round_down_to_tick(min(max_px, maker_best_ask - tick, maker_best_bid + max_improve_ticks*tick)).
   Post-only always. Reduce-only when the order reduces |maker_pos|.
4. **Requote**: event-driven on book updates (debounced by `loop_interval_s`).
   - PROTECTIVE reprice (resting price now violates the edge limit, or would cross): immediately, ignores min lifetime.
   - PROTECTIVE amend that fails (retryable or not, except a would-cross) -> cancel the quote.
   - Improvement reprice: only if moved >= `reprice_threshold_bps` and order age >= `min_quote_lifetime_s`.
   - RESIZE (size-only amend): only when the quote exceeds what is still needed to reach the goal
     (and the position, for reduce-only), and also subject to `min_quote_lifetime_s`.
   - Use `modify_order` (Arcus supports amend) else cancel/replace.
   - No quoting while either venue's private (orders / fills) stream is unhealthy (`private_ok()`); the
     maker venue's resting quote is pulled. Untracked live `fq...` quotes (e.g. a place that landed after its
     cancel) are cancelled when seen on the order stream or in the `orphan_sweep_s` open-order sweep.
5. **Hedge** (Hedger): every maker fill (WS) wakes the hedger. `delta = maker_pos + taker_pos + inflight`.
   `q = -delta` rounded toward zero to the taker step; if tradeable, send taker IOC with
   `worst_price = touch * (1 ± max_slippage_bps)`. Taker fills arrive asynchronously (Lighter
   settles via its sequencer; fills come over WS) and reduce `inflight`. In-flight entries
   expire after `hedge_timeout_s` (counted from when the send returned) and are reconciled from REST
   positions. Fills an adapter replays from a snapshot after a private-stream reconnect (`Fill.recovered`) are
   not added blindly (a reconcile may already have adopted them): they trigger a two-read reconcile.
6. **Cycle**: OPENING reaches goal -> HOLDING (for `hold_seconds`, optionally through the next
   funding settlement) -> CLOSING (if `churn`) -> reaches floor -> OPENING (apply pending flip if flat).
   Within `protect_funding_window_s` before (and `protect_funding_post_s` after) a settlement CLOSING is
   deferred so a position is held at the funding timestamp.

Volume per cycle per venue = `2 * (target - floor)` notional. Cost per cycle ~ `-min_edge` (>= 0 by
construction, minus hedge slippage from the taker latency).

## Safety

- Hedging is ALWAYS allowed (risk-reducing) even when quoting is paused/halted.
- Pause quoting when: either book stale (`stale_book_s`), venue mids diverge (`max_divergence_bps`),
  unhedged delta above `max_unhedged_notional`, margin ratio above `max_margin_ratio` or free collateral
  below `clip / leverage * min_free_collateral_mult` on a venue (opening only), RWA market outside regular
  hours (`skip_rwa_off_hours`), circuit breaker tripped.
- Margin ratio (maintenance / equity, liquidation at 1.0) at `reduce_margin_ratio` -> halt + the risk
  manager's reducer (`Engine.close_all("taker")`: reduce-only IOCs on both legs of every pair, independent of
  halt / edge / book gates), retried while the ratio stays high. Diverging venue equities
  (`max_equity_divergence_frac`) are reported: the bot never moves collateral between venues.
- Unhedged delta persisting beyond `unhedged_timeout_s` -> cancel quotes, aggressive hedge with
  2x slippage; beyond 3x timeout -> emergency flatten of the maker leg with a taker order, then no quoting
  for `emergency_cooldown_s`; `max_emergencies_per_hour` emergencies -> halt.
- Arcus dead man's switch (venue-side cancel-all) refreshed periodically.
- Daily loss limit -> halt (cancel quotes; optionally flatten). Daily PnL = cash flow + fees + funding of
  today's fills + each venue's net base at that venue's own mid (the structural cross-venue basis of a hedged
  entry is not a loss).
- Startup: cancel stale maker orders, read positions, hedge any pre-existing delta.
- Shutdown (SIGINT/SIGTERM/SIGHUP, also during `close-all`): cancel quotes (plus `cancel_all` per maker
  market, verified by polling open orders), flush hedgers until no maker fill arrived for `shutdown_settle_s`,
  alert on any unconfirmed cancel or unhedged pair; `close_on_exit` (only after a completed start)
  flattens both legs. `close-all` ignores the daily-loss halt, switches a maker close to taker mode when the
  risk manager halts, and fails (non-zero exit) for positions in configured markets no pair could close.

## Event-loop hygiene

Everything runs on one asyncio loop, so long synchronous work delays hedges and book handling. The store
keeps per-(venue, symbol, hour) aggregate rows next to the raw fills / funding rows (same transaction), so a
24 h or all-time summary sums O(hours) rows plus the raw rows of at most two partial hours; file databases
add a query-only connection and the stats report / daily-loss check read it in a worker thread
(`asyncio.to_thread`).

## Rate-limit budgets

- Lighter Standard: 60 REST req / rolling minute INCLUDING sendTx. Hedges get priority; reads
  (positions/balances/funding) use WS where possible and otherwise keep a reserve for hedges.
  Paper mode emulates it (`paper.emulate_rate_limits`): order actions share the inner adapter's request
  bucket and a 40 tx/min limiter and fail retryably after `tx_budget_wait_s`.
  Every maker fill costs one hedge tx: traders pull their quotes while the taker venue's `budget_health()`
  (lower of the shared-bucket and tx-cap shares) is below `strategy.min_taker_budget`, resuming at 1.5x.
- Arcus: see `specs/arcus-trading.md` (order/cancel pools); amend instead of cancel/replace.
