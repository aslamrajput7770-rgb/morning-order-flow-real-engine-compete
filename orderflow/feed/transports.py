"""Concrete transports.

`WebSocketTransport` is the only place in the package that touches a socket, and
it imports its client library lazily. That keeps the ingestion layer
dependency-free: importing `orderflow.feed` never pulls in a WebSocket library,
and the offline path (replay, tests, backtests) never needs one.

The other transports here are file- and list-backed, for recorded sessions.
"""

from __future__ import annotations

import json
import ssl
import threading
from pathlib import Path
from typing import Iterable

from .connector import HEARTBEAT_MESSAGE, ROOT_URI, Transport

#: Lazily imported so that importing this module never requires websocket-client.
_ABNF = None


def _abnf():
    """The websocket-client ABNF opcode table, imported on first use."""
    global _ABNF
    if _ABNF is None:
        from websocket import ABNF  # type: ignore

        _ABNF = ABNF
    return _ABNF


class RecordingTransport:
    """Tees every received frame to a recorder, then delegates.

    Records the connection's raw stream rather than per-instrument trades. That
    is deliberate: a frame does not announce which instrument it belongs to
    until it is decoded, and the decoder already exists on the replay side. So
    the recorder stores exactly what the socket delivered, and replay routes it
    through the same router as a live session. Nothing is interpreted twice, and
    a decoder fix retroactively applies to every recording ever taken.

    `recv` is the only method that records. Sending a subscription is not
    market data, and replaying it would only confuse the replay transport.
    """

    def __init__(self, inner, recorder, key: str, clock=None) -> None:
        self._inner = inner
        self._recorder = recorder
        self._key = key
        self._clock = clock
        self.recorded = 0

    def __getattr__(self, name):
        # is_live, send, heartbeat, close and anything else pass through. The
        # connector checks `is_live` on the transport, so this must be the
        # inner value and not the wrapper's.
        return getattr(self._inner, name)

    def connect(self) -> None:
        self._inner.connect()

    def send(self, data, binary: bool = False) -> None:
        self._inner.send(data, binary=binary)

    def recv(self, timeout: float | None = None):
        frame = self._inner.recv(timeout=timeout)
        if frame is not None:
            ts = self._clock() if self._clock is not None else None
            try:
                self._recorder.write(self._key, frame, ts=ts)
                self.recorded += 1
            except Exception:
                # A full disk must not kill the feed. Losing the recording is
                # bad; losing the live session is worse, and the caller can see
                # the frame count stop advancing.
                pass
        return frame

    def close(self) -> None:
        self._inner.close()


class WebSocketTransport:
    """A real SmartStream socket, using `websocket-client` if present.

    Install with `pip install websocket-client`. Credentials come from the
    Angel One login response: `auth_token` (the JWT), `api_key` and
    `client_code` (your Angel One id). They go in HTTP headers, not the URL.

    This class deliberately does the minimum: connect, send, receive, close.
    Subscription logic, decoding and routing all live above it, so they are
    testable without a socket.
    """

    is_live = True

    def __init__(
        self,
        auth_token: str,
        api_key: str,
        client_code: str,
        feed_token: str,
        uri: str = ROOT_URI,
        timeout: float = 10.0,
    ) -> None:
        self.auth_token = auth_token
        self.api_key = api_key
        self.client_code = client_code
        self.feed_token = feed_token
        self.uri = uri
        self.timeout = timeout
        self._ws = None
        self._lock = threading.Lock()

    def auth_headers(self) -> dict:
        """The handshake headers, as the official SDK sends them.

        The JWT goes in bare. The official SDK sets
        `"Authorization": self.auth_token` with no scheme prefix, and the word
        "Bearer" appears nowhere in its WebSocket module. The REST login
        endpoint does use "Bearer", which is what makes this easy to get wrong
        by pattern-matching the two.
        """
        return {
            "Authorization": self.auth_token,
            "x-api-key": self.api_key,
            "x-client-code": self.client_code,
            "x-feed-token": self.feed_token,
        }

    def connect(self) -> None:
        try:
            import websocket  # type: ignore
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise ImportError(
                "WebSocketTransport needs the 'websocket-client' package: "
                "pip install websocket-client"
            ) from exc

        headers = self.auth_headers()
        self._ws = websocket.create_connection(
            self.uri,
            header=[f"{k}: {v}" for k, v in headers.items()],
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
            timeout=self.timeout,
        )

    def send(self, data: bytes | str, binary: bool = False) -> None:
        """Send a frame. Subscription JSON is TEXT; the server expects that.

        websocket-client's `send` defaults to `opcode=OPCODE_TEXT`, and it only
        emits a binary frame if asked. The official SDK sends
        `json.dumps(request_data)`, so a str: an OPCODE_TEXT frame. Passing
        bytes here would silently produce OPCODE_BINARY instead, and the server
        would not read it as a subscription.
        """
        if self._ws is None:
            raise RuntimeError("transport is not connected")
        with self._lock:
            abnf = _abnf()
            opcode = abnf.OPCODE_BINARY if binary else abnf.OPCODE_TEXT
            self._ws.send(data, opcode=opcode)

    def heartbeat(self) -> None:
        """A protocol-level PING frame.

        websocket-client answers the server's PONG inside `recv_data_frame`, so
        no manual PONG handling is needed here. This mirrors
        `run_forever(ping_interval=10)` in the official SDK.
        """
        if self._ws is None:
            raise RuntimeError("transport is not connected")
        with self._lock:
            self._ws.ping()

    def recv(self, timeout: float | None = None) -> bytes | None:
        if self._ws is None:
            raise RuntimeError("transport is not connected")
        if timeout is not None:
            self._ws.settimeout(timeout)
        try:
            return self._ws.recv()
        except Exception:
            # Timeout or closed socket: the caller decides whether to reconnect.
            return None

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            finally:
                self._ws = None


class ShardTransport:
    """Replays `.oft.gz` shards written by `orderflow.recorder`.

    This is the live-backtest transport: it hands the same frames to the same
    connector, router and session chart that a live socket feeds, so a replay
    exercises the real pipeline rather than a parallel one. No trades are
    injected below this point.

    One instance replays one instrument's shards in timestamp order. Shards are
    ordered by their filename, which is the hour stamp, so a month of files
    replays chronologically without reading them all into memory first.
    """

    is_live = False

    def __init__(self, root: str | Path, key: str | None = None,
                 shards: Iterable[str | Path] | None = None) -> None:
        from ..recorder import iter_shards

        self.root = Path(root)
        self.key = key
        if shards is not None:
            self._paths = [Path(s) for s in shards]
        else:
            self._paths = list(iter_shards(self.root, key))
        self._records = []
        self._i = 0
        self.sent: list[bytes] = []
        self.heartbeats = 0
        self.shards_read = 0
        self.torn_shards: list[str] = []

    def connect(self) -> None:
        from ..recorder import read_shard

        self._records = []
        for path in self._paths:
            try:
                self._records.extend(read_shard(path))
                self.shards_read += 1
            except Exception:
                # One unreadable shard must not stop a month-long replay.
                self.torn_shards.append(str(path))
        self._i = 0

    def send(self, data: bytes | str, binary: bool = False) -> None:
        self.sent.append(data if isinstance(data, bytes) else data.encode("utf-8"))

    def heartbeat(self) -> None:
        self.heartbeats += 1

    def recv(self, timeout: float | None = None):
        from ..recorder import TYPE_TEXT

        if self._i >= len(self._records):
            return None
        rec = self._records[self._i]
        self._i += 1
        return rec.payload.decode("utf-8", "replace") if rec.type == TYPE_TEXT else rec.payload

    def close(self) -> None:
        self._records = []
        self._i = 0


class FileTransport:
    """Replays frames recorded to disk, one frame per line.

    Lines are either `bin:<hex>` for a binary frame or `txt:<text>` for a
    heartbeat. Recording a live session once and keeping it as a fixture is the
    cheapest way to make ingestion regressions reproducible.
    """

    is_live = False

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._frames: list[bytes | str] = []
        self._i = 0
        self.sent: list[bytes] = []
        self.heartbeats = 0

    def connect(self) -> None:
        self._frames = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("bin:"):
                self._frames.append(bytes.fromhex(line[4:]))
            elif line.startswith("txt:"):
                self._frames.append(line[4:])
        self._i = 0

    def send(self, data: bytes | str, binary: bool = False) -> None:
        self.sent.append(data if isinstance(data, bytes) else data.encode("utf-8"))

    def heartbeat(self) -> None:
        """No control channel in a recording; count it so tests can assert."""
        self.heartbeats += 1

    def recv(self, timeout: float | None = None):
        if self._i >= len(self._frames):
            return None
        f = self._frames[self._i]
        self._i += 1
        return f

    def close(self) -> None:
        pass


def record_frames(frames: Iterable[bytes | str], path: str | Path) -> Path:
    """Write frames in the `FileTransport` format."""
    p = Path(path)
    lines = []
    for f in frames:
        if isinstance(f, (bytes, bytearray)):
            lines.append("bin:" + bytes(f).hex())
        else:
            lines.append("txt:" + str(f))
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def login(
    api_key: str,
    client_code: str,
    pin: str,
    totp: str,
    base_url: str = "https://apiconnect.angelone.in",
    session=None,
) -> dict:
    """Minimal REST login. Returns the tokens the socket needs.

    Kept here for completeness; it is the one function that needs a network and
    is therefore never exercised by the offline tests. Use the official SDK's
    login if you prefer, and pass its `jwtToken`, `feedToken` and your api key
    into `WebSocketTransport`.
    """
    import urllib.request

    body = json.dumps(
        {"clientcode": client_code, "password": pin, "totp": totp}
    ).encode()
    req = urllib.request.Request(
        f"{base_url}/rest/auth/angelbroking/user/v1/loginByPassword",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": api_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read())
    if not payload.get("status"):
        raise RuntimeError(f"login failed: {payload.get('message')}")
    return payload["data"]
