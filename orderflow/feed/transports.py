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

    def connect(self) -> None:
        try:
            import websocket  # type: ignore
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise ImportError(
                "WebSocketTransport needs the 'websocket-client' package: "
                "pip install websocket-client"
            ) from exc

        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "x-api-key": self.api_key,
            "x-client-code": self.client_code,
            "x-feed-token": self.feed_token,
        }
        self._ws = websocket.create_connection(
            self.uri,
            header=[f"{k}: {v}" for k, v in headers.items()],
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
            timeout=self.timeout,
        )

    def send(self, data: bytes) -> None:
        if self._ws is None:
            raise RuntimeError("transport is not connected")
        with self._lock:
            self._ws.send(data)

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

    def send(self, data: bytes) -> None:
        self.sent.append(data)

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
