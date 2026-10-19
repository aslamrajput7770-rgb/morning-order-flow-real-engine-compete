"""RFC 6238 TOTP, standard library only.

Angel One login needs a 6-digit TOTP. `pyotp` is the usual choice, but the
algorithm is thirty lines of stdlib and keeping it here means the live runner
has no dependency beyond the WebSocket client itself.

Verified against the RFC 6238 Appendix B test vectors in the test suite.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct
import time

#: Default period and digit count for Angel One.
DEFAULT_PERIOD = 30
DEFAULT_DIGITS = 6


def totp(
    secret_base32: str,
    at_time: float | None = None,
    period: int = DEFAULT_PERIOD,
    digits: int = DEFAULT_DIGITS,
    algorithm: str = "sha1",
) -> str:
    """Return the TOTP for `at_time` (default: now).

    `secret_base32` is the shared secret as it appears in the authenticator app,
    with or without padding and spaces.
    """
    if at_time is None:
        at_time = time.time()

    key = _decode_base32(secret_base32)
    counter = int(at_time) // period
    digest = hmac.new(key, struct.pack(">Q", counter), _hash_name(algorithm)).digest()
    offset = digest[-1] & 0x0F
    code = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(code % (10**digits)).zfill(digits)


def _hash_name(name: str) -> str:
    """Validate the algorithm and return its canonical name for `hmac.new`."""
    try:
        return hashlib.new(name).name
    except ValueError as exc:
        raise ValueError(f"unsupported hash {name!r}") from exc


def _decode_base32(secret: str) -> bytes:
    cleaned = secret.replace(" ", "").replace("-", "").upper()
    # Restore any stripped padding: base32 wants a length that is a multiple of 8.
    pad = (-len(cleaned)) % 8
    try:
        return base64.b32decode(cleaned + "=" * pad)
    except Exception as exc:
        raise ValueError("invalid base32 TOTP secret") from exc
