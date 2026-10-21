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


def subscribe_message_text(action: int, subscriptions: Iterable[Subscription],
                           correlation_id: str = "of1") -> str:
    """The subscribe frame as text, which is what the wire carries.

    The official SDK sends `json.dumps(request_data)`, a str, so
    websocket-client emits an OPCODE_TEXT frame. It does NOT send a binary
    frame, and it does NOT prefix the JSON with any length header. Sending the
    same JSON as bytes produces an OPCODE_BINARY frame instead, which the
    server does not parse as a subscription.
    """
    return subscribe_message(action, subscriptions, correlation_id).decode("utf-8")


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------
class Transport(Protocol):
    """Minimal transport contract. Inject a WebSocket client or a replay."""

    #: True for a live socket, where `recv` returning None means "no data yet"
    #: and must not end the stream. False for a replay, where None means
    #: "exhausted". A live feed that treats an idle timeout as end-of-stream
    #: dies on the first quiet minute, which looks like a market outage.
    is_live: bool

    def connect(self) -> None: ...

    def send(self, data: bytes | str, binary: bool = False) -> None: ...

    def heartbeat(self) -> None:
        """Send a heartbeat. A no-op on transports without a control channel."""
        ...

    def recv(self, timeout: float | None = None) -> bytes | None: ...

    def close(self) -> None: ...


class ReplayTransport:
    """Replays recorded frames. No socket, no dependency.

    `frames` are the raw bytes of each WebSocket message, in order. A `None`
    entry simulates a timeout with no data. Heartbeat strings are passed through
    as `str` to exercise the skip path.
    """

    is_live = False

    def __init__(self, frames: Iterable[bytes | str | None]) -> None:
        self._frames = list(frames)
        self._i = 0
        self.sent: list[bytes] = []
        self.heartbeats = 0
        self.connected = False
        self.closed = False

    def connect(self) -> None:
        self.connected = True

    def send(self, data: bytes | str, binary: bool = False) -> None:
        self.sent.append(data if isinstance(data, bytes) else data.encode("utf-8"))

    def heartbeat(self) -> None:
        """A replay has no control channel; record it so tests can assert on it."""
        self.heartbeats += 1

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


# Returned by `TickRouter.route` for a tick whose token the mapper does not
# know. This is distinct from `None`, which means "a known token, but this tick
# carried no new trade" (a quote-only update or the first baseline tick).
# Collapsing the two made every baseline and every quote update look like an
# unmapped token, which the rehearsal and capture diagnostics treat as a
# configuration fault.
UNMAPPED = object()


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

    def namespace_for(self, raw: RawTick) -> TokenNamespace | None:
        """The namespace a tick's exchange maps to, or None if unknown."""
        return self._namespace_by_exchange.get(raw.exchange_type)

    def route(self, raw: RawTick):
        """Route one tick.

        Returns a `RoutedTrade`, or `None` when the tick carried no new trade
        (a quote-only update, or the first baseline tick of a stream), or the
        `UNMAPPED` sentinel when the token is not in the mapper. The caller must
        tell the last two apart: one is normal, the other is a config fault.
        """
        namespace = self.namespace_for(raw)
        if namespace is None:
            return UNMAPPED
        try:
            instrument = self.mapper.by_token(namespace, int(raw.token))
        except Exception:
            # An unmapped token is a configuration gap, not a crash. Surface it
            # as a distinct outcome; the connector decides how to count it.
            return UNMAPPED
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
    #: Ticks for a token that is in the master but not in the plan. Live never
    #: sends these; a replay shard carries every token it recorded.
    not_subscribed: int = 0
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
        #: (namespace, token) pairs the plan actually asked for. A live server
        #: sends nothing else, but a replay shard holds every token that was
        #: recorded on the connection, so without this filter a one-instrument
        #: replay routes the whole shard and trades instruments the caller never
        #: asked for.
        self._subscribed: set[tuple[TokenNamespace, int]] = {
            (sub.namespace, int(sub.token)) for sub in plan.all()
        }

    # ------------------------------------------------------------- lifecycle
    def _subscribe(self, transport: Transport, index: int) -> None:
        session = self.plan.sessions[index]
        if not session:
            return
        # TEXT frame, matching the official SDK's `wsapp.send(json.dumps(...))`.
        transport.send(subscribe_message_text(1, session, correlation_id=f"of{index}"))

    def run(
        self,
        on_trade: Callable[[RoutedTrade], None],
        max_frames: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        max_seconds: float | None = None,
        reconnect: bool = True,
        max_reconnects: int = 5,
    ) -> ConnectorStats:
        """Consume frames until the transport is exhausted or a limit is hit.

        On every (re)connect the subscriptions are re-sent. A socket that
        reconnects without resubscribing produces silence, which is
        indistinguishable from a quiet market.

        `max_seconds` is the wall-clock budget for a live run: the loop stops
        when it elapses, so a live session cannot run forever by accident.
        `reconnect` retries a dropped socket up to `max_reconnects` times with
        backoff, resubscribing each time.
        """
        started = clock()
        for index, session in enumerate(self.plan.sessions):
            if not session:
                continue
            attempt = 0
            while True:
                transport = self.transport_factory(index)
                try:
                    transport.connect()
                except Exception as exc:
                    self.stats.protocol_errors += 1
                    if self.on_error is not None:
                        self.on_error(exc)
                    attempt += 1
                    if not reconnect or attempt > max_reconnects:
                        break
                    self.stats.reconnects += 1
                    time.sleep(min(2**attempt, 30))
                    continue
                self._subscribe(transport, index)
                try:
                    self._consume(
                        transport, on_trade, max_frames, clock,
                        deadline=None if max_seconds is None else started + max_seconds,
                    )
                except Exception as exc:
                    # A live socket dies in many ways (reset, SSL, decode of a
                    # torn frame). Reconnect rather than losing the session.
                    self.stats.protocol_errors += 1
                    if self.on_error is not None:
                        self.on_error(exc)
                finally:
                    transport.close()

                attempt += 1
                # Reconnect only applies to a live socket. A replay is exhausted
                # on purpose, so retrying it would sleep through the backoff and
                # then replay nothing.
                if not reconnect or not getattr(transport, "is_live", False):
                    break
                if attempt > max_reconnects:
                    break
                if max_seconds is not None and clock() - started >= max_seconds:
                    break
                self.stats.reconnects += 1
                time.sleep(min(2**attempt, 30))
        return self.stats

    def _consume(
        self,
        transport: Transport,
        on_trade: Callable[[RoutedTrade], None],
        max_frames: int | None,
        clock: Callable[[], float],
        deadline: float | None = None,
    ) -> None:
        idle_since: float | None = None
        while True:
            if max_frames is not None and self.stats.frames >= max_frames:
                return
            if deadline is not None and clock() >= deadline:
                return
            frame = transport.recv(timeout=self.heartbeat_interval)
            if frame is None:
                if not getattr(transport, "is_live", False):
                    return
                # Live socket: None is an idle timeout, not end-of-stream.
                if idle_since is None:
                    idle_since = clock()
                self.maybe_heartbeat(transport, clock)
                continue
            idle_since = None
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
        # A token the plan never asked for is not our tick. Live cannot produce
        # one; a replay shard carries every token recorded on the connection.
        # Drop it before the reader sees it, so its volume-delta state cannot
        # even be seeded by a stream we are not trading.
        namespace = self.router.namespace_for(raw)
        if namespace is None or (namespace, int(raw.token)) not in self._subscribed:
            # Tell a configuration gap (token absent from the master, a fault)
            # apart from a replay artefact (a real instrument we did not
            # subscribe to). The two must not share a counter: one means the
            # master is stale, the other is normal for a replay.
            if self._known_token(namespace, raw.token):
                self.stats.not_subscribed += 1
            else:
                self.stats.unmapped += 1
            return
        routed = self.router.route(raw)
        if routed is UNMAPPED:
            self.stats.unmapped += 1
            return
        if routed is None:
            # A known token with no new trade: quote-only update or the first
            # baseline tick. Normal, and not a trade.
            return
        self.stats.trades += 1
        on_trade(routed)

    def _known_token(self, namespace: TokenNamespace | None, token) -> bool:
        """True when the token exists in the master (regardless of the plan)."""
        if namespace is None:
            return False
        try:
            self.mapper.by_token(namespace, int(token))
        except Exception:
            return False
        return True

    def maybe_heartbeat(
        self,
        transport: Transport,
        clock: Callable[[], float] = time.monotonic,
    ) -> bool:
        """Send a heartbeat if the interval elapsed. Returns True when sent.

        This sends a WebSocket protocol-level PING frame, not a text message
        containing the word "ping". The official SDK relies on
        `run_forever(ping_interval=10)`, which is exactly that: websocket-client
        sends a PING control frame and the server answers with PONG. A text
        "ping" is not a heartbeat, gets no reply, and lets a dead socket look
        alive while the feed is silent.
        """
        now = clock()
        if now - self._last_heartbeat >= self.heartbeat_interval:
            transport.heartbeat()
            self._last_heartbeat = now
            return True
        return False
