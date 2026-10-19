"""Configuration: YAML file (non-secret settings) + environment / .env (secrets).

Load with ``load_config(path)``. Secrets are NEVER read from YAML; they come from env vars
(see ``.env.example``).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

Venue = Literal["arcus", "lighter"]


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- venues
class ArcusConfig(_Base):
    rest_url: str = "https://api.arcus.xyz"  # testnet: https://api.testnet.arcus.xyz
    ws_url: str = "wss://api.arcus.xyz/v1/ws"  # testnet: wss://api.testnet.arcus.xyz/v1/ws
    chain_id: int = 4663  # EIP-712 chainId for API-key registration (testnet: 46630)
    account_index: int = 0  # subaccount 0..9
    # Secrets (from env): ARCUS_ADDRESS, ARCUS_API_PRIVATE_KEY
    address: str | None = None
    api_private_key: str | None = Field(default=None, repr=False)
    request_timeout_s: float = 10.0
    dead_mans_switch_s: float = 60.0  # scheduleCancel deadline (5..300 s); refreshed at 1/3 of it (0 = off)
    good_til_days: int = 180  # goodTilTime lead; modify requires >= ~1 month remaining
    ack_timeout_s: float = 3.0  # how long place/modify waits for the WS `orders` frame before returning PENDING
    requote_budget_per_s: float = 20.0  # local cap on order-pool actions (protects the 20k starting pool)

    @property
    def has_credentials(self) -> bool:
        return bool(self.address and self.api_private_key)


class LighterConfig(_Base):
    url: str = "https://mainnet.zklighter.elliot.ai"  # Lighter RH: https://api.rh.lighter.xyz
    ws_url: str = "wss://mainnet.zklighter.elliot.ai/stream"  # Lighter RH: wss://api.rh.lighter.xyz/stream
    chain_id: int = 304  # mainnet 304, testnet 300, Lighter RH 466324
    # Secrets (from env): LIGHTER_API_PRIVATE_KEY, LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX
    account_index: int | None = None
    api_key_index: int | None = None
    api_private_key: str | None = Field(default=None, repr=False)
    request_timeout_s: float = 10.0
    # Standard accounts: ONE bucket of 60 requests / rolling minute per IP and L1 address shared by
    # REST reads AND sendTx/sendTxBatch (REST or WS). Keep headroom.
    max_requests_per_minute: int = 55
    read_reserve: int = 25  # REST reads may only run while at least this many slots stay free for txs
    auth_token_ttl_s: float = 6 * 3600  # WS/REST auth token lifetime (server max 8 h)
    ws_ping_interval_s: float = 30.0  # server drops clients idle > ~120 s

    @property
    def has_credentials(self) -> bool:
        return self.api_private_key is not None and self.account_index is not None and self.api_key_index is not None


# --------------------------------------------------------------------------- strategy
class MarketConfig(_Base):
    """Per-market settings. Any ``None`` field falls back to ``StrategyConfig`` defaults."""

    symbol: str  # canonical (base asset), e.g. "BTC"
    enabled: bool = True
    arcus_symbol: str | None = None  # override auto mapping, e.g. "BTC-USD"
    lighter_symbol: str | None = None  # override auto mapping, e.g. "BTC"
    target_notional: Decimal | None = None
    order_notional: Decimal | None = None
    leverage: int | None = None
    min_edge_bps: Decimal | None = None
    max_slippage_bps: Decimal | None = None


class StrategyConfig(_Base):
    maker_venue: Venue = "arcus"
    taker_venue: Venue = "lighter"
    markets: list[MarketConfig] = Field(
        default_factory=lambda: [MarketConfig(symbol="BTC"), MarketConfig(symbol="ETH"), MarketConfig(symbol="SOL")]
    )

    # --- sizing (USD notional)
    target_notional: Decimal = Decimal("2000")  # |position| per leg at the top of a cycle
    order_notional: Decimal = Decimal("500")  # maker clip size
    leverage: int = 5

    # --- pricing
    # Maker quote is placed so that hedging on the taker venue at its current VWAP yields at
    # least ``min_edge_bps`` (can be negative to buy volume at a small cost).
    min_edge_bps: Decimal = Decimal("0.5")
    max_slippage_bps: Decimal = Decimal("8")  # taker hedge worst price vs touch
    reprice_threshold_bps: Decimal = Decimal("0.5")  # move the maker quote if fair price moved this much
    requote_buffer_bps: Decimal = Decimal("0.3")  # quote this far behind the edge limit to absorb noise
    min_quote_lifetime_s: float = 0.5  # don't amend (improvement reprices) faster than this
    max_requotes_per_min: int = 120  # per market, protective + improvement amends
    max_improve_ticks: int = 1_000_000  # how far inside the maker book we may go (capped by post-only)

    # --- basis (structural price gap between venues, e.g. USD vs USDT index ~10 bps on crypto)
    use_basis: bool = True  # price the edge vs taker_price*(1+basis) instead of the raw taker price
    basis_halflife_s: float = 600.0  # EMA half-life of maker_mid/taker_mid - 1
    basis_warmup_s: float = 30.0  # don't quote until the basis estimate has this much data

    # --- hedging
    hedge_aggregate_ms: float = 250.0  # coalesce maker fills before sending one taker hedge
    hedge_timeout_s: float = 8.0  # in-flight hedge without a terminal update -> reconcile from REST/WS
    hedge_retry_backoff_s: float = 0.5

    # --- cycle / volume churn
    churn: bool = True  # open -> hold -> close -> reopen to generate volume
    churn_floor_frac: Decimal = Decimal("0")  # close down to this fraction of target (0 = flat)
    hold_seconds: float = 0.0  # minimum time at target before closing
    hold_through_funding: bool = False  # wait for the next funding settlement before closing
    protect_funding_window_s: float = 120.0  # don't close below floor / flip this close to a settlement

    # --- funding direction
    direction_mode: Literal["auto", "long_maker", "short_maker"] = "auto"
    funding_refresh_s: float = 60.0
    flip_hysteresis_apr: Decimal = Decimal("0.02")  # need 2 % APR better to flip direction

    # --- loop timings
    loop_interval_s: float = 0.25
    reconcile_interval_s: float = 15.0

    @model_validator(mode="after")
    def _check(self) -> "StrategyConfig":
        if self.maker_venue == self.taker_venue:
            raise ValueError("maker_venue and taker_venue must differ")
        if not (Decimal(0) <= self.churn_floor_frac < Decimal(1)):
            raise ValueError("churn_floor_frac must be in [0, 1)")
        if self.order_notional <= 0 or self.target_notional <= 0:
            raise ValueError("notional sizes must be positive")
        return self

    def for_market(self, m: MarketConfig) -> "ResolvedMarket":
        def pick(name: str):
            v = getattr(m, name)
            return v if v is not None else getattr(self, name)

        return ResolvedMarket(
            symbol=m.symbol.upper(),
            arcus_symbol=m.arcus_symbol,
            lighter_symbol=m.lighter_symbol,
            target_notional=pick("target_notional"),
            order_notional=pick("order_notional"),
            leverage=pick("leverage"),
            min_edge_bps=pick("min_edge_bps"),
            max_slippage_bps=pick("max_slippage_bps"),
        )


class ResolvedMarket(_Base):
    symbol: str
    arcus_symbol: str | None
    lighter_symbol: str | None
    target_notional: Decimal
    order_notional: Decimal
    leverage: int
    min_edge_bps: Decimal
    max_slippage_bps: Decimal


# --------------------------------------------------------------------------- risk
class RiskConfig(_Base):
    max_unhedged_notional: Decimal = Decimal("150")  # USD delta tolerated before emergency action
    unhedged_timeout_s: float = 5.0  # how long a delta above min-size may persist
    max_total_notional: Decimal = Decimal("20000")  # sum |position notional| over all markets per venue
    max_divergence_bps: Decimal = Decimal("60")  # pause if |maker_mid/taker_mid - 1| exceeds this (absolute)
    max_basis_dev_bps: Decimal = Decimal("5")  # pause if the instant basis deviates from its EMA by more
    stale_book_s: float = 5.0  # cancel quotes if a book is older than this
    max_margin_ratio: Decimal = Decimal("0.6")  # maintenance/equity per venue: stop opening above this
    reduce_margin_ratio: Decimal = Decimal("0.8")  # actively reduce positions above this
    max_daily_loss: Decimal = Decimal("100")  # USD realized+fees+funding; stop the bot if breached
    max_consecutive_errors: int = 20
    close_on_exit: bool = False  # flatten both legs on shutdown
    skip_rwa_off_hours: bool = True  # RWA pairs only while Arcus isOutsideRth == false
    emergency_flatten: bool = True  # flatten the maker leg with a taker order if hedging keeps failing


# --------------------------------------------------------------------------- misc
class PaperConfig(_Base):
    initial_balance: Decimal = Decimal("10000")
    maker_latency_ms: dict[str, float] = Field(default_factory=lambda: {"arcus": 50.0, "lighter": 0.0})
    taker_latency_ms: dict[str, float] = Field(default_factory=lambda: {"arcus": 50.0, "lighter": 300.0})
    # Fee overrides for simulation (fractions). None = use venue market metadata.
    maker_fee: dict[str, Decimal] = Field(default_factory=dict)
    taker_fee: dict[str, Decimal] = Field(default_factory=dict)


class MonitoringConfig(_Base):
    report_interval_s: float = 60.0
    # Secrets (from env): TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    telegram_bot_token: str | None = Field(default=None, repr=False)
    telegram_chat_id: str | None = None


class AppConfig(_Base):
    mode: Literal["paper", "live"] = "paper"
    log_level: str = "INFO"
    data_dir: Path = Path("./data")
    arcus: ArcusConfig = Field(default_factory=ArcusConfig)
    lighter: LighterConfig = Field(default_factory=LighterConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    paper: PaperConfig = Field(default_factory=PaperConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    def enabled_markets(self) -> list[ResolvedMarket]:
        return [self.strategy.for_market(m) for m in self.strategy.markets if m.enabled]


# --------------------------------------------------------------------------- loading
_ENV_MAP: dict[tuple[str, str], tuple[str, type]] = {
    ("arcus", "address"): ("ARCUS_ADDRESS", str),
    ("arcus", "api_private_key"): ("ARCUS_API_PRIVATE_KEY", str),
    ("arcus", "account_index"): ("ARCUS_ACCOUNT_INDEX", int),
    ("lighter", "api_private_key"): ("LIGHTER_API_PRIVATE_KEY", str),
    ("lighter", "account_index"): ("LIGHTER_ACCOUNT_INDEX", int),
    ("lighter", "api_key_index"): ("LIGHTER_API_KEY_INDEX", int),
    ("monitoring", "telegram_bot_token"): ("TELEGRAM_BOT_TOKEN", str),
    ("monitoring", "telegram_chat_id"): ("TELEGRAM_CHAT_ID", str),
}


def load_config(path: str | os.PathLike | None = None, *, env_file: str | os.PathLike | None = ".env") -> AppConfig:
    """Load YAML config (optional) and overlay secrets from the environment / .env file."""
    if env_file:
        try:
            from dotenv import load_dotenv

            load_dotenv(env_file, override=False)
        except ImportError:  # pragma: no cover
            pass

    raw: dict = {}
    if path is not None:
        p = Path(path)
        if p.exists():
            raw = yaml.safe_load(p.read_text()) or {}
        else:
            raise FileNotFoundError(f"config file not found: {p}")

    for (section, key), (env, typ) in _ENV_MAP.items():
        val = os.environ.get(env)
        if val is None or val.strip() == "":
            continue
        raw.setdefault(section, {})
        raw[section][key] = typ(val.strip())

    if os.environ.get("FARMER_MODE"):
        raw["mode"] = os.environ["FARMER_MODE"].strip()

    return AppConfig.model_validate(raw)
