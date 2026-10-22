"""Order-flow analytics engine.

    from orderflow import TickGrid, Trade, Side, FootprintChart

    grid = TickGrid(0.25)
    chart = FootprintChart(grid, bar_seconds=60)
    chart.add(Trade(ts=0.0, price=100.0, size=5, side=Side.BUY))
    print(chart.current.render("bidask"))

Layer map
---------
core       : Trade, Side, TickGrid, PriceLevel, ValueArea, Imbalance, Divergence
footprint  : FootprintBar, FootprintChart (bid/ask, delta, POC, imbalances)
profile    : Profile, ProfileBuilder, naked_pocs (POC/VAH/VAL, virgin POC)
delta      : cumulative delta candles, delta profile, divergence
tape       : Tape, big trades, counters
dom        : BookSnapshot, LiquidityMap, absorption, SuperDOM
indicators : order-flow VWAP, pressure, market analyzer
"""

from .core import (
    Divergence,
    Imbalance,
    ImbalanceSide,
    PriceLevel,
    Side,
    TickGrid,
    Trade,
    ValueArea,
)
from .ticks import OffGridTick
from .delta import (
    Candle,
    DeltaState,
    bar_delta_series,
    cumulative_delta_series,
    delta_profile,
    detect_divergence,
    value_area_of_delta,
)
from .dom import (
    Absorption,
    BookSnapshot,
    LiquidityMap,
    SuperDOM,
    detect_absorption,
    liquidity_shift,
)
from .footprint import FootprintBar, FootprintChart
from .indicators import (
    InstrumentSignal,
    PressureGauge,
    VWAPBand,
    VWAPPoint,
    buy_sell_volume,
    market_analyzer,
    order_flow_vwap,
)
from .instruments import (
    BANKNIFTY_FUT,
    MCX_GOLD_FUT,
    MCX_SILVER_FUT,
    NIFTY_FUT,
    RELIANCE_EQ,
    Exchange,
    Instrument,
    Segment,
    SessionKind,
    TokenMapper,
    TokenNamespace,
    UnknownToken,
    build_indian_token_map,
)
from .profile import (
    NakedPoc,
    Profile,
    ProfileBuilder,
    ProfileMode,
    naked_pocs,
)
from .render import (
    ansi_footprint,
    footprint_html,
    heatmap_color,
    write_html,
)
from .session import (
    IST,
    Market,
    MarketCalendar,
    SessionTracker,
    SessionWindow,
    UnionCalendar,
    us_daylight_saving,
)
from .session_chart import SessionChart, SessionState
from .session_store import SessionStore
from .strategy import Signal, SignalEngine, StrategyConfig, evaluate_latest
from .paper import (
    CostModel,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    PaperBroker,
    Position,
    RoundTrip,
)
from .autotrade import AutoTrader, AutoTraderConfig, InstrumentState
from .supervisor import AlwaysOnFeed, FeedHealth, SupervisorConfig
from .tape import (
    Counter,
    Tape,
    TapePrint,
    big_trade_summary,
)

__version__ = "0.1.0"

__all__ = [
    # core
    "Trade",
    "Side",
    "TickGrid",
    "PriceLevel",
    "ValueArea",
    "Imbalance",
    "ImbalanceSide",
    "Divergence",
    # footprint
    "FootprintBar",
    "FootprintChart",
    # profile
    "Profile",
    "ProfileBuilder",
    "ProfileMode",
    "NakedPoc",
    "naked_pocs",
    # delta
    "Candle",
    "DeltaState",
    "cumulative_delta_series",
    "bar_delta_series",
    "delta_profile",
    "detect_divergence",
    "value_area_of_delta",
    # tape
    "Tape",
    "TapePrint",
    "big_trade_summary",
    "Counter",
    # dom
    "BookSnapshot",
    "LiquidityMap",
    "Absorption",
    "detect_absorption",
    "SuperDOM",
    "liquidity_shift",
    # indicators
    "VWAPPoint",
    "VWAPBand",
    "order_flow_vwap",
    "PressureGauge",
    "InstrumentSignal",
    "market_analyzer",
    "buy_sell_volume",
    # render
    "heatmap_color",
    "ansi_footprint",
    "footprint_html",
    "write_html",
    # ticks
    "OffGridTick",
    # instruments
    "Exchange",
    "Segment",
    "SessionKind",
    "Instrument",
    "NIFTY_FUT",
    "BANKNIFTY_FUT",
    "RELIANCE_EQ",
    "MCX_GOLD_FUT",
    "MCX_SILVER_FUT",
    "TokenNamespace",
    "TokenMapper",
    "UnknownToken",
    "build_indian_token_map",
    # session
    "IST",
    "Market",
    "MarketCalendar",
    "UnionCalendar",
    "SessionWindow",
    "SessionTracker",
    "us_daylight_saving",
    "SessionChart",
    "SessionState",
    # strategy
    "Signal",
    "SignalEngine",
    "StrategyConfig",
    "evaluate_latest",
    # paper trading
    "PaperBroker",
    "CostModel",
    "Order",
    "OrderType",
    "OrderStatus",
    "Fill",
    "Position",
    "RoundTrip",
    # autonomous trading
    "AutoTrader",
    "AutoTraderConfig",
    "InstrumentState",
    # always-on feed
    "AlwaysOnFeed",
    "FeedHealth",
    "SupervisorConfig",
    # persistence
    "SessionStore",
]
