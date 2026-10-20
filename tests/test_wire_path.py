"""The connector's outgoing wire behaviour, on the path live runs use.

The conformance tests pin the frame builders. These run the real
`SmartStreamConnector` against a transport that records what it was handed, so
the wiring from the connector down to the socket is covered too. A builder that
is correct but never called, or called with the wrong argument, is the failure
this catches.
"""

from __future__ import annotations

import json

from orderflow.feed import (
    SubscriptionMode,
    TokenNamespace,
    build_plan,
)
from orderflow import build_indian_token_map
from orderflow.feed.connector import SmartStreamConnector


class RecordingTransport:
    """Records sends and counts control frames. Never opens a socket.

    `is_live=False` means `recv` returning None ends the stream, so the
    connector drains and stops. That is what the frame-capture tests want. The
    heartbeat tests need `is_live=True`, where None means "idle, keep waiting",
    and they drive the clock forward so the wall-clock budget is reached.
    """

    def __init__(self, frames=(), is_live=False):
        self._frames = list(frames)
        self._i = 0
        self.is_live = is_live
        self.sent = []
        self.heartbeats = 0
        self.connected = False
        self.closed = False

    def connect(self):
        self.connected = True

    def send(self, data, binary=False):
        self.sent.append((data, binary))

    def heartbeat(self):
        self.heartbeats += 1

    def recv(self, timeout=None):
        if self._i < len(self._frames):
            f = self._frames[self._i]
            self._i += 1
            return f
        return None

    def close(self):
        self.closed = True


def conn_for(transport, pairs=None, mode=SubscriptionMode.SNAP_QUOTE, **kw):
    pairs = pairs or [(TokenNamespace.NSE_FO, 48704)]
    plan = build_plan(pairs, mode=mode)
    return SmartStreamConnector(
        plan, build_indian_token_map(), lambda i: transport, **kw)


class TestSubscribeGoesOutAsText:
    def test_connector_sends_a_text_frame_not_binary(self):
        """This is the exact call the live run makes.

        If the connector handed bytes to the transport, websocket-client would
        emit an OPCODE_BINARY frame and the server would never register the
        subscription.
        """
        t = RecordingTransport()
        conn_for(t).run(on_trade=lambda rt: None, reconnect=False)
        assert t.sent, "the connector sent nothing"
        payload, binary = t.sent[0]
        assert binary is False, "subscribe was sent as a binary frame"
        assert isinstance(payload, str), "subscribe payload must be text JSON"

    def test_subscribe_payload_parses_as_the_sdk_shape(self):
        t = RecordingTransport()
        conn_for(t).run(on_trade=lambda rt: None, reconnect=False)
        frame = json.loads(t.sent[0][0])
        assert frame["action"] == 1
        assert frame["params"]["mode"] == 3
        entry = frame["params"]["tokenList"][0]
        assert entry["exchangeType"] == 2
        assert entry["tokens"] == ["48704"]

    def test_real_token_is_the_one_subscribed(self):
        """48704 is the Nifty future in the master; 26000 is the index."""
        t = RecordingTransport()
        conn_for(t, pairs=[(TokenNamespace.NSE_FO, 48704)]).run(
            on_trade=lambda rt: None, reconnect=False)
        frame = json.loads(t.sent[0][0])
        assert frame["params"]["tokenList"][0]["tokens"] == ["48704"]

    def test_multi_exchange_subscription_groups_by_exchange_type(self):
        t = RecordingTransport()
        conn_for(t, pairs=[(TokenNamespace.NSE_FO, 48704),
                           (TokenNamespace.MCX_FO, 495213)]).run(
            on_trade=lambda rt: None, reconnect=False)
        frame = json.loads(t.sent[0][0])
        by_ex = {e["exchangeType"]: e["tokens"]
                 for e in frame["params"]["tokenList"]}
        assert by_ex[2] == ["48704"]
        assert by_ex[5] == ["495213"]


class SteppingClock:
    """A monotonic clock that advances on every read.

    A clock that stays at 0.0 never reaches `max_seconds`, so the live loop
    spins forever. Each `recv` returning None also has to move time, or an
    idle socket never times out.
    """

    def __init__(self, step=6.0):
        self.t = 0.0
        self.step = step

    def __call__(self):
        self.t += self.step
        return self.t


class TestHeartbeatOnTheLivePath:
    def test_connector_calls_heartbeat_on_the_transport(self):
        """Not `send("ping")`: the transport decides how to ping."""
        t = RecordingTransport(is_live=True)
        conn = conn_for(t, heartbeat_interval=10.0)
        conn.run(on_trade=lambda rt: None, reconnect=False,
                 max_seconds=60, clock=SteppingClock())
        assert t.heartbeats > 0, "no control frame was sent while idle"

    def test_no_text_ping_is_ever_sent(self):
        t = RecordingTransport(is_live=True)
        conn = conn_for(t, heartbeat_interval=10.0)
        conn.run(on_trade=lambda rt: None, reconnect=False,
                 max_seconds=60, clock=SteppingClock())
        for payload, _ in t.sent[1:]:
            assert str(payload).strip() != "ping"


class TestReconnectResubscribes:
    def test_a_reconnect_sends_the_subscription_again(self):
        """A socket that reconnects without resubscribing is silent, and
        silence is indistinguishable from a quiet market."""
        t = RecordingTransport(is_live=True)
        conn = conn_for(t, heartbeat_interval=10.0)
        conn.run(on_trade=lambda rt: None, reconnect=True,
                 max_seconds=60, clock=SteppingClock(), max_reconnects=2)
        subscribe_frames = [
            p for p, _ in t.sent
            if isinstance(p, str) and "tokenList" in p
        ]
        assert len(subscribe_frames) >= 1
        for p in subscribe_frames:
            assert json.loads(p)["action"] == 1
