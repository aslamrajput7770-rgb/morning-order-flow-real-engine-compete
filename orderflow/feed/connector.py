"""SmartStream V2 connector: subscription planning, routing, reconnect.

The transport is injected, never imported. `SmartStreamConnector` drives any
object with `connect()`, `send(bytes)`, `recv()` and `close()`. That is what
lets the same ingestion code run against:

  * a real WebSocket (bring your own client library)
  * a recorded packet file (`ReplayTransport`), for tests and backtests
  * a synthetic generator, for soak testing

Nothing in this module opens a socket, so it has no third-party dependency and
is fully testable offline.

Operational facts that shape the design
---------------------------------------
* A WebSocket session allows **1000 token subscriptions**. Subscribing to the
  same token in two modes counts as two. A footprint needs volume, so QUOTE or
  SNAP_QUOTE is the mode; DEPTH gives the full 20-level book but no trade
  volume, so a footprint typically wants SNAP_QUOTE and a ladder wants DEPTH.
* A client (one Angel One account) may hold **three concurrent connections**.
  Beyond 1000 tokens the plan must be split across connections, which this
  module does.
* The client sends the literal ``ping`` every 10 seconds and must ignore the
  server's non-binary replies. Treating a heartbeat as a tick is a classic
  source of corrupt data.
* On reconnect the subscriptions must be re-sent. A reconnected socket with no
  subscriptions is silent, which looks exactly like a dead market.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, Protocol

from ..core import Trade
from ..instruments import Exchange, Instrument, TokenMapper, TokenNamespace
from .protocol import (
    ExchangeType,
    ProtocolError,
    RawTick,
    SubscriptionMode,
    decode_packet,
    is_heartbeat,
)
from .tick_reader import TickReader, TickTrade

#: Hard limits published by Angel One for SmartStream V2.
MAX_TOKENS_PER_SESSION = 1000
MAX_CONCURRENT_CONNECTIONS = 3
HEARTBEAT_MESSAGE = "ping"
HEARTBEAT_INTERVAL_SECONDS = 10
ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream"


# --------------------------------------------------------------------------
# Subscription planning
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Subscription:
    namespace: TokenNamespace
    token: int
    mode: SubscriptionMode

    @property
    def key(self) -> tuple[TokenNamespace, int, SubscriptionMode]:
        return (self.namespace, self.token, self.mode)


def _exchange_type(namespace: TokenNamespace) -> ExchangeType:
    return {
        TokenNamespace.NSE_EQ: ExchangeType.NSE_CM,
        TokenNamespace.NSE_FO: ExchangeType.NSE_FO,
        TokenNamespace.BSE_EQ: ExchangeType.BSE_CM,
        TokenNamespace.BSE_FO: ExchangeType.BSE_FO,
        TokenNamespace.MCX_FO: ExchangeType.MCX_FO,
        TokenNamespace.NCDEX_FO: ExchangeType.NCX_FO,
    }[namespace]


@dataclass(slots=True)
class SubscriptionPlan:
    """A set of subscriptions split across at most three connections."""

    sessions: list[list[Subscription]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(len(s) for s in self.sessions)

    def all(self) -> list[Subscription]:
        return [sub for session in self.sessions for sub in session]


def build_plan(
    instruments: Iterable[tuple[TokenNamespace, int]],
    mode: SubscriptionMode = SubscriptionMode.SNAP_QUOTE,
) -> SubscriptionPlan:
    """Split subscriptions across connections, respecting the 1000 limit.

    Duplicate `(namespace, token, mode)` triples are collapsed: the exchange
    ignores redundant subscriptions and they would otherwise consume quota.
    """
    seen: set[tuple[TokenNamespace, int, SubscriptionMode]] = set()
    subs: list[Subscription] = []
    for ns, token in instruments:
        s = Subscription(ns, int(token), mode)
        if s.key in seen:
            continue
        seen.add(s.key)
        subs.append(s)

    sessions = [
        subs[i : i + MAX_TOKENS_PER_SESSION]
        for i in range(0, len(subs), MAX_TOKENS_PER_SESSION)
    ]
    if len(sessions) > MAX_CONCURRENT_CONNECTIONS:
        raise ValueError(
            f"{len(subs)} subscriptions need {len(sessions)} connections but "
            f"Angel One allows {MAX_CONCURRENT_CONNECTIONS} per client; "
            "subscribe in LTP mode for the rest or use a second account"
        )
    return SubscriptionPlan(sessions=sessions)


def subscribe_message(
    action: int,
    subscriptions: Iterable[Subscription],
    correlation_id: str = "of1",
) -> bytes:
    """Build the JSON subscription frame, grouped by exchange type.

    `action` is 1 to subscribe and 0 to unsubscribe.
    """
    if len(correlation_id) > 10:
        raise ValueError("correlation_id must be at most 10 characters")

    grouped: dict[int, list[str]] = {}
    for s in subscriptions:
        grouped.setdefault(int(_exchange_type(s.namespace)), []).append(str(s.token))

    mode = next(iter(subscriptions)).mode if not isinstance(subscriptions, list) else None
    subs_list = list(subscriptions)
    if not subs_list:
        raise ValueError("no subscriptions to send")
    mode = subs_list[0].mode
    for s in subs_list:
        if s.mode is not mode:
            raise ValueError("all subscriptions in one frame must share a mode")

    payload = {
        "correlationID": correlation_id,
        "action": action,
        "params": {
            "mode": int(mode),
            "tokenList": [
                {"exchangeType": et, "tokens": toks} for et, toks in grouped.items()
            ],
        },
    }
    return json.dumps(payload).encode("utf-8")


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------
class Transport(Protocol):
    """Minimal transport contract. Inject a WebSocket client or a replay."""

    def connect(self) -> None: ...

    def send(self, data: bytes) -> None: ...

    def recv(self, timeout: float | None = None) -> bytes | None: ...

    def close(self) -> None: ...


class ReplayTransport:
    """Replays recorded frames. No socket, no dependency.

    `frames` are the raw bytes of each WebSocket message, in order. A `None`
    entry simulates a timeout with no data. Heartbeat strings are passed through
    as `str` to exercise the skip path.
    """

    def __init__(self, frames: Iterable[bytes | str | None]) -> None:
        self._frames = list(frames)
        self._i = 0
        self.sent: list[bytes] = []
        self.connected = False
        self.closed = False

    def connect(self) -> None:
        self.connected = True

    def send(self, data: bytes) -> None:
        self.sent.append(data)

    def recv(self, timeout: float | None = None):
        if self._i >= len(self._frames):
            return None
        frame = self._frames[self._i]
        self._i += 1
        return frame

    def close(self) -> None:
        self.closed = True


# --------------------------------------------------------------------------
# Routing
# --------------------------------------------------------------------------
@dataclass(slots=True)
class RoutedTrade:
    """A trade with the instrument it belongs to."""

    instrument: Instrument
    trade: Trade
    source: str


class TickRouter:
    """Routes decoded ticks to per-instrument readers.

    Each token gets its own `TickReader`, because the volume-delta state and the
    last-price state are per instrument. Sharing one reader across tokens makes
    every instrument's delta the difference between two unrelated prices.
    """

    def __init__(self, mapper: TokenMapper) -> None:
        self.mapper = mapper
        self._readers: dict[tuple[TokenNamespace, int], TickReader] = {}
        self._namespace_by_exchange: dict[ExchangeType, TokenNamespace] = {
            ExchangeType.NSE_CM: TokenNamespace.NSE_EQ,
            ExchangeType.NSE_FO: TokenNamespace.NSE_FO,
            ExchangeType.BSE_CM: TokenNamespace.BSE_EQ,
            ExchangeType.BSE_FO: TokenNamespace.BSE_FO,
            ExchangeType.MCX_FO: TokenNamespace.MCX_FO,
            ExchangeType.NCX_FO: TokenNamespace.NCDEX_FO,
        }

    def reader_for(self, namespace: TokenNamespace, token: int) -> TickReader:
        key = (namespace, int(token))
        r = self._readers.get(key)
        if r is None:
            r = self._readers[key] = TickReader(str(token))
        return r

    def route(self, raw: RawTick) -> RoutedTrade | None:
        """Route one tick. Returns None for quote-only updates and unknown tokens."""
        namespace = self._namespace_by_exchange.get(raw.exchange_type)
        if namespace is None:
            return None
        try:
            instrument = self.mapper.by_token(namespace, int(raw.token))
        except Exception:
            # An unmapped token is a configuration gap, not a crash. Count it
            # at the connector level; here it is simply not ours.
            return None
        reader = self.reader_for(namespace, int(raw.token))
        tt = reader.read(raw)
        if tt is None:
            return None
        return RoutedTrade(instrument=instrument, trade=tt.trade, source=tt.source.value)

    def stats(self) -> dict[str, dict[str, int]]:
        return {
            f"{ns.value}:{tok}": {
                "prints": r.state.prints,
                "volume_resets": r.state.volume_resets,
            }
            for (ns, tok), r in self._readers.items()
        }


# --------------------------------------------------------------------------
# Connector
# --------------------------------------------------------------------------
@dataclass(slots=True)
class ConnectorStats:
    frames: int = 0
    ticks: int = 0
    trades: int = 0
    heartbeats: int = 0
    protocol_errors: int = 0
    unmapped: int = 0
    reconnects: int = 0


class SmartStreamConnector:
    """Drives a transport, decodes frames, and emits routed trades.

    The connector owns no socket. Feed it a transport and a subscription plan:

        plan = build_plan(mapper.tokens_for("NIFTY"), SubscriptionMode.SNAP_QUOTE)
        conn = SmartStreamConnector(plan, mapper, transport_factory)
        conn.run(on_trade=lambda rt: chart.add(rt.trade))
    """

    def __init__(
        self,
        plan: SubscriptionPlan,
        mapper: TokenMapper,
        transport_factory: Callable[[int], Transport],
        heartbeat_interval: float = HEARTBEAT_INTERVAL_SECONDS,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self.plan = plan
        self.mapper = mapper
        self.transport_factory = transport_factory
        self.heartbeat_interval = heartbeat_interval
        self.on_error = on_error
        self.router = TickRouter(mapper)
        self.stats = ConnectorStats()
        self._last_heartbeat = 0.0

    # ------------------------------------------------------------- lifecycle
    def _subscribe(self, transport: Transport, index: int) -> None:
        session = self.plan.sessions[index]
        if not session:
            return
        transport.send(subscribe_message(1, session, correlation_id=f"of{index}"))

    def run(
        self,
        on_trade: Callable[[RoutedTrade], None],
        max_frames: int | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> ConnectorStats:
        """Consume frames until the transport is exhausted or max_frames is hit.

        On every (re)connect the subscriptions are re-sent. A socket that
        reconnects without resubscribing produces silence, which is
        indistinguishable from a quiet market.
        """
        for index, session in enumerate(self.plan.sessions):
            if not session:
                continue
            transport = self.transport_factory(index)
            transport.connect()
            self._subscribe(transport, index)
            try:
                self._consume(transport, on_trade, max_frames, clock)
            finally:
                transport.close()
        return self.stats

    def _consume(
        self,
        transport: Transport,
        on_trade: Callable[[RoutedTrade], None],
        max_frames: int | None,
        clock: Callable[[], float],
    ) -> None:
        while True:
            if max_frames is not None and self.stats.frames >= max_frames:
                return
            frame = transport.recv(timeout=self.heartbeat_interval)
            if frame is None:
                return
            self.stats.frames += 1
            self._handle_frame(frame, on_trade, transport, clock)

    def _handle_frame(
        self,
        frame,
        on_trade: Callable[[RoutedTrade], None],
        transport: Transport,
        clock: Callable[[], float],
    ) -> None:
        if is_heartbeat(frame):
            # Non-binary or a short frame: server heartbeat. Skip, never parse.
            self.stats.heartbeats += 1
            return
        try:
            raw = decode_packet(frame)
        except ProtocolError as exc:
            self.stats.protocol_errors += 1
            if self.on_error is not None:
                self.on_error(exc)
            return
        self.stats.ticks += 1
        routed = self.router.route(raw)
        if routed is None:
            self.stats.unmapped += 1
            return
        self.stats.trades += 1
        on_trade(routed)

    def maybe_heartbeat(
        self,
        transport: Transport,
        clock: Callable[[], float] = time.monotonic,
    ) -> bool:
        """Send `ping` if the interval elapsed. Returns True when sent."""
        now = clock()
        if now - self._last_heartbeat >= self.heartbeat_interval:
            transport.send(HEARTBEAT_MESSAGE.encode("utf-8"))
            self._last_heartbeat = now
            return True
        return False
