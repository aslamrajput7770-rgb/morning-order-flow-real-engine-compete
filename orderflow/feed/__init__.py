"""Data ingestion layer for the Indian market (Angel One SmartStream V2).

Layering, and why it is split this way
--------------------------------------
    bytes -> protocol.py    decode, no I/O
          -> tick_reader.py cumulative volume -> incremental prints, side inference
          -> connector.py   subscription planning, routing, reconnect, heartbeat
          -> session_chart  footprint, delta, profile, daily reset

Every layer above the socket is pure and offline-testable. `ReplayTransport`
replaces the socket with recorded frames, so the whole path can be exercised in
tests with no network and no broker credentials.

What this layer does NOT do
---------------------------
Angel One SmartStream is not an order-by-order feed. It does not tell you the
aggressor side of a print, and it does not give a trade id. Side is inferred by
the quote rule (print vs best bid/ask) and the tick rule, and left `UNKNOWN`
when neither applies. That inference is the honest limit of this data source; a
footprint built from it is a good approximation, not a reconstruction of the
order book.
"""

from .connector import (
    HEARTBEAT_MESSAGE,
    MAX_CONCURRENT_CONNECTIONS,
    MAX_TOKENS_PER_SESSION,
    ROOT_URI,
    ConnectorStats,
    ReplayTransport,
    RoutedTrade,
    SmartStreamConnector,
    Subscription,
    SubscriptionPlan,
    TickRouter,
    Transport,
    build_plan,
    subscribe_message,
)
from .protocol import (
    DEFAULT_PRICE_SCALE,
    PriceScaleError,
    MIN_PACKET_LENGTH,
    ProtocolError,
    RawTick,
    SubscriptionMode,
    ExchangeType,
    decode_packet,
    decode_stream,
    is_heartbeat,
    price_scale,
)
from .tick_reader import (
    SideSource,
    SnapshotModeUnsupported,
    TickReader,
    TickReaderState,
    TickTrade,
    require_volume_capable_mode,
)
from .simulator import TickSpec, encode_stream, encode_tick
from .transports import FileTransport, WebSocketTransport, record_frames
from .totp import totp
from .diagnostics import (
    Diagnostics,
    VERDICT_INSUFFICIENT,
    VERDICT_INVERTED,
    VERDICT_NO_BOOK,
    VERDICT_OK,
    report,
    scan_capture,
    scan_frames,
    scan_ticks,
)
from ..instruments import TokenNamespace

__all__ = [
    # re-exported for convenience
    "TokenNamespace",
    # protocol
    "ExchangeType",
    "SubscriptionMode",
    "RawTick",
    "ProtocolError",
    "PriceScaleError",
    "decode_packet",
    "decode_stream",
    "is_heartbeat",
    "price_scale",
    "MIN_PACKET_LENGTH",
    "DEFAULT_PRICE_SCALE",
    # reader
    "TickReader",
    "TickReaderState",
    "TickTrade",
    "SideSource",
    "SnapshotModeUnsupported",
    "require_volume_capable_mode",
    # connector
    "SmartStreamConnector",
    "ConnectorStats",
    "SubscriptionPlan",
    "Subscription",
    "build_plan",
    "subscribe_message",
    "TickRouter",
    "RoutedTrade",
    "Transport",
    "ReplayTransport",
    "ROOT_URI",
    "HEARTBEAT_MESSAGE",
    "MAX_TOKENS_PER_SESSION",
    "MAX_CONCURRENT_CONNECTIONS",
    # simulator
    "TickSpec",
    "encode_tick",
    "encode_stream",
    # transports
    "WebSocketTransport",
    "FileTransport",
    "record_frames",
    # auth helper
    "totp",
    # capture diagnostics
    "Diagnostics",
    "scan_capture",
    "scan_frames",
    "scan_ticks",
    "report",
    "VERDICT_OK",
    "VERDICT_INVERTED",
    "VERDICT_NO_BOOK",
    "VERDICT_INSUFFICIENT",
]
