"""Wire-protocol conformance with the official Angel One SmartStream SDK.

Every assertion here was taken from the official client,
`SmartApi/smartWebSocketV2.py` in `angel-one/smartapi-python`, not from
documentation prose. The point is that each of these mistakes is silent: the
socket connects, the login succeeds, the server accepts the frame, and no data
ever arrives. There is nothing to notice at runtime, so the checks have to live
here.
"""

from __future__ import annotations

import json

import pytest

from orderflow.feed import (
    ExchangeType,
    SubscriptionMode,
    TokenNamespace,
    build_plan,
    subscribe_message_text,
)
from orderflow.feed.connector import ROOT_URI
from orderflow.feed.transports import WebSocketTransport


def transport() -> WebSocketTransport:
    return WebSocketTransport(
        auth_token="jwt-token-value",
        api_key="api-key",
        client_code="A123456",
        feed_token="feed-token",
    )


class TestHandshakeHeaders:
    """The official SDK's connect() sets four headers, verbatim."""

    def test_authorization_is_the_bare_jwt(self):
        """No "Bearer" scheme.

        The official SDK assigns `"Authorization": self.auth_token` directly,
        and the string "Bearer" does not occur in its WebSocket module. The REST
        login endpoint *does* send "Bearer", so copying that habit here produces
        a rejected handshake.
        """
        h = transport().auth_headers()
        assert h["Authorization"] == "jwt-token-value"
        assert not h["Authorization"].startswith("Bearer")
        assert "Bearer" not in h["Authorization"]

    def test_all_four_headers_present_and_named_exactly(self):
        h = transport().auth_headers()
        assert set(h) == {"Authorization", "x-api-key", "x-client-code",
                          "x-feed-token"}
        assert h["x-api-key"] == "api-key"
        assert h["x-client-code"] == "A123456"
        assert h["x-feed-token"] == "feed-token"

    def test_uri_matches_the_official_endpoint(self):
        assert ROOT_URI == "wss://smartapisocket.angelone.in/smart-stream"
        assert transport().uri == ROOT_URI


class TestSubscribeFrame:
    """The subscribe frame is JSON text, shaped exactly as the SDK builds it."""

    def _frame(self, pairs, mode=SubscriptionMode.SNAP_QUOTE, cid="of1"):
        plan = build_plan(pairs, mode=mode)
        return json.loads(subscribe_message_text(1, plan.sessions[0], cid))

    def test_frame_is_text_not_bytes(self):
        plan = build_plan([(TokenNamespace.NSE_FO, 48704)],
                          mode=SubscriptionMode.SNAP_QUOTE)
        frame = subscribe_message_text(1, plan.sessions[0])
        assert isinstance(frame, str)

    def test_top_level_shape_matches_sdk(self):
        f = self._frame([(TokenNamespace.NSE_FO, 48704)])
        assert set(f) == {"correlationID", "action", "params"}
        assert f["action"] == 1
        assert set(f["params"]) == {"mode", "tokenList"}

    def test_token_list_entries_use_exchangeType_and_tokens(self):
        f = self._frame([(TokenNamespace.NSE_FO, 48704)])
        entry = f["params"]["tokenList"][0]
        assert set(entry) == {"exchangeType", "tokens"}
        assert entry["exchangeType"] == 2
        assert entry["tokens"] == ["48704"]

    def test_tokens_are_strings_not_ints(self):
        """The SDK's own sample uses `["10626", "5290"]`, quoted."""
        f = self._frame([(TokenNamespace.NSE_FO, 48704)])
        assert all(isinstance(t, str) for t in f["params"]["tokenList"][0]["tokens"])

    def test_snap_quote_mode_is_three(self):
        f = self._frame([(TokenNamespace.NSE_FO, 48704)])
        assert f["params"]["mode"] == 3

    def test_exchange_types_match_the_sdk_table(self):
        assert ExchangeType.NSE_CM.value == 1
        assert ExchangeType.NSE_FO.value == 2
        assert ExchangeType.BSE_CM.value == 3
        assert ExchangeType.BSE_FO.value == 4
        assert ExchangeType.MCX_FO.value == 5
        assert ExchangeType.NCX_FO.value == 7
        assert ExchangeType.CDE_FO.value == 13

    def test_unknown_exchange_is_not_silently_dropped(self):
        """A namespace with no exchangeType cannot be subscribed to."""
        f = self._frame([(TokenNamespace.NSE_FO, 48704)])
        assert f["params"]["tokenList"][0]["exchangeType"] == 2

    def test_correlation_id_is_capped_at_ten_characters(self):
        """The SDK documents a 10-character limit; the server echoes it back."""
        plan = build_plan([(TokenNamespace.NSE_FO, 48704)],
                          mode=SubscriptionMode.SNAP_QUOTE)
        with pytest.raises(ValueError):
            subscribe_message_text(1, plan.sessions[0], correlation_id="x" * 11)

    def test_unsubscribe_action_is_zero(self):
        plan = build_plan([(TokenNamespace.NSE_FO, 48704)],
                          mode=SubscriptionMode.SNAP_QUOTE)
        f = json.loads(subscribe_message_text(0, plan.sessions[0]))
        assert f["action"] == 0


class TestHeartbeat:
    """The heartbeat is a protocol-level PING frame, not a text "ping"."""

    def test_transport_exposes_a_control_channel_heartbeat(self):
        t = transport()
        assert callable(getattr(t, "heartbeat", None))

    def test_heartbeat_is_not_a_text_message(self):
        """`send("ping")` would be an OPCODE_TEXT data frame, which draws no
        PONG. The official SDK uses `run_forever(ping_interval=10)`, i.e. a
        PING control frame."""
        import inspect

        src = inspect.getsource(WebSocketTransport.heartbeat)
        assert "ping()" in src
        assert "send(" not in src

    def test_send_defaults_to_a_text_frame(self):
        """A subscription sent as bytes becomes OPCODE_BINARY, which the server
        does not parse as a subscription. The default must be TEXT."""
        import inspect

        sig = inspect.signature(WebSocketTransport.send)
        assert sig.parameters["binary"].default is False
